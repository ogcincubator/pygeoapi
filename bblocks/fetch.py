import argparse
import json
import re
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from shapely.geometry import shape as shapely_shape
import yaml

import sys

DEFAULT_DATA_DIR = Path('data')
STAC_SUBDIR = 'stac'

DEFAULT_PYGEOAPI_CONFIG_FN = 'pygeoapi.config.yml'
OUTPUT_SUBDIR = 'bblocks'


def safe_filename(s: str):
    return re.sub(r'[^a-zA-Z0-9._-]+', '_', s)


def get_register_name(url: str):
    parsed = urlparse(url)

    def clean(s: str):
        return re.sub(r'[^a-zA-Z0-9._-]+', '_', s)

    return f"{clean(parsed.hostname)}_{clean(parsed.path)}"


def fetch_json(url):
    r = requests.get(url)
    r.raise_for_status()
    return r.json()


def get_envelop_bbox(collections: Iterable[dict]) \
        -> tuple[float, float, float, float]:
    envelope_bbox = None, None, None, None

    def get_updated_bbox(g):
        bbox = shapely_shape(g).bounds
        return (
            bbox[0] if envelope_bbox[0] is None
            else min(envelope_bbox[0], bbox[0]),
            bbox[1] if envelope_bbox[1] is None
            else min(envelope_bbox[1], bbox[1]),
            bbox[2] if envelope_bbox[2] is None
            else max(envelope_bbox[2], bbox[2]),
            bbox[3] if envelope_bbox[3] is None
            else max(envelope_bbox[3], bbox[3]),
        )

    for entry in collections:
        if geom := entry.get('geometry'):
            envelope_bbox = get_updated_bbox(geom)
        elif features := entry.get('features'):
            for feature in features:
                if geom := feature.get('geometry'):
                    envelope_bbox = get_updated_bbox(geom)
    return envelope_bbox


def process_register(register_url: str, register_fn: Path,
                     data_dir: Path, fallback_sparql: str | None = None,
                     force=False):
    new_register = fetch_json(register_url)

    output_dir = data_dir / OUTPUT_SUBDIR
    stac_dir = data_dir / STAC_SUBDIR

    needs_update = not register_fn.is_file() or force

    if not needs_update:
        with open(register_fn) as f:
            last_register = json.load(f)
        needs_update = last_register != new_register

    if not needs_update:
        return False

    fallback_sparql = new_register.get('sparqlEndpoint', fallback_sparql)

    output_resources = {}

    for bblock_entry in new_register['bblocks']:
        bblock = fetch_json(bblock_entry['documentation']['json-full']['url'])
        bblock_feature_collections = {}
        bblock_stac_items = []
        for i, example in enumerate(bblock.get('examples', [])):
            for snippet in example.get('snippets', []):
                if snippet.get('language') in ('json',):
                    snippet_code = json.loads(snippet['code'])
                    if not isinstance(snippet_code, dict):
                        continue
                    if not (snippet_type := snippet_code.get('type')):
                        continue
                    if snippet_type == 'Feature':
                        if snippet_code.get('stac_version'):
                            # STAC item
                            bblock_stac_items.append({
                                'ref': snippet.get('ref'),
                                'code': snippet_code
                            })
                        else:
                            fc = bblock_feature_collections.setdefault('', {})
                            if not fc:
                                fc.update({
                                    'type': 'FeatureCollection',
                                    'features': []
                                })
                            fc['features'].append(snippet_code)
                    elif snippet_code == 'FeatureCollection':
                        bblock_feature_collections[str(i)] = snippet_code

        if bblock_feature_collections:

            bbox = get_envelop_bbox(bblock_feature_collections.values())

            output_resource = {
                'type': 'collection',
                'visibility': 'default',
                'title': bblock['name'],
                'description': bblock.get('abstract', ''),
                'keywords': bblock.get('tags', []),
                'extents': {
                    'spatial': {
                        'bbox': bbox,
                    },
                },
            }

            if ld_context := bblock.get('ldContext'):
                ld_config = {
                    'inject_verbatim_context': True,
                    'replace_id_field': 'id',
                    'context': [
                        ld_context
                    ]
                }
                if fallback_sparql:
                    ld_config['fallback_sparql_endpoint'] = fallback_sparql
                output_resource['linked-data'] = ld_config

            providers = []
            for key, fc in bblock_feature_collections.items():
                collection_fn = (output_dir.joinpath(bblock['itemIdentifier'])
                                 .with_suffix('.geojson'))
                if key:
                    collection_fn = collection_fn.with_stem(
                        collection_fn.stem + '_' + key
                    )
                collection_fn.parent.mkdir(parents=True, exist_ok=True)
                with open(collection_fn, 'w') as f:
                    json.dump(fc, f, indent=2)
                providers.append({
                    'type': 'feature',
                    'name': 'GeoJSON',
                    'data': str(Path('/') / collection_fn),
                })
            output_resource['providers'] = providers

            output_resources[bblock['itemIdentifier']] = output_resource

        if bblock_stac_items:
            stac_dir = stac_dir / bblock['itemIdentifier']
            stac_dir.mkdir(parents=True, exist_ok=True)

            added_items = set()

            extensions = set()
            catalog = {
                'id': bblock['itemIdentifier'],
                'title': f"{bblock['name']} catalog",
                'description': f"STAC Catalog for examples in "
                               f"{bblock['name']} building block.",
                'type': 'Catalog',
                'stac_version': '1.0.0',
                'stac_extensions': None,
                'links': []
            }

            for item in bblock_stac_items:
                item_ref = item['ref']
                item = item['code']
                item_id = safe_filename(item['id'])
                while item_id in added_items:
                    if m := re.match(r'(.+)_([0-9]+)$', item_id):
                        item_id = f"{m.group(1)}_{int(m.group(2)) + 1}"
                    else:
                        item_id = f"{item_id}_2"
                id_fn = safe_filename(item_id)
                item_dir = stac_dir / id_fn
                item_dir.mkdir(parents=True, exist_ok=True)

                if item_ref:
                    for asset in item.get('assets', {}).values():
                        if not re.match(r'^https?://', asset['href']):
                            # relative link
                            asset['href'] = urljoin(item_ref, asset['href'])

                with open(item_dir / f'{id_fn}.json', 'w') as f:
                    json.dump(item, f, indent=2)
                catalog['links'].append({
                    'rel': 'item',
                    'href': f'./{id_fn}/{id_fn}.json'
                })
                extensions.update(item.get('stac_extensions', ()))

            catalog['stac_extensions'] = list(extensions)
            with open(stac_dir / 'catalog.json', 'w') as f:
                json.dump(catalog, f, indent=2)

            output_resource = {
                'type': 'stac-collection',
                'title': catalog['title'],
                'description': catalog['description'],
                'providers': [
                    {
                        'type': 'stac',
                        'name': 'Hateoas',
                        'data': str(Path('/') / stac_dir),
                        'file_types': [
                            'catalog.json'
                        ]
                    }
                ]
            }
            if ld_context := bblock.get('ldContext'):
                ld_config = {
                    'inject_verbatim_context': True,
                    'replace_id_field': 'id',
                    'context': [
                        ld_context
                    ]
                }
                if fallback_sparql:
                    ld_config['fallback_sparql_endpoint'] = fallback_sparql
                output_resource['linked-data'] = ld_config
            output_resources[bblock['itemIdentifier']] = output_resource

    return output_resources, new_register


def _main():
    parser = argparse.ArgumentParser(
        prog='bblocks-pygeoapi-fetch',
        description='Fetches OGC Building Blocks '
                    'examples as pygeoapi resources',
    )
    parser.add_argument(
        'register',
        help='Register URL (register.json) to fetch',
        nargs='+',
    )
    parser.add_argument(
        '-c', '--config-file',
        help=f'pygeoapi config file ("{DEFAULT_PYGEOAPI_CONFIG_FN}" '
             f'by default)',
        default=DEFAULT_PYGEOAPI_CONFIG_FN,
    )
    parser.add_argument(
        '-s', '--fallback-sparql',
        help='Fallback SPARQL endpoint to retrieve definitions '
             '(when direct resolution fails and the register '
             'does not specify one)',
    )
    parser.add_argument(
        '-d', '--data-dir',
        help=f'Directory to write data to ("{DEFAULT_DATA_DIR}" by default)',
        default=str(DEFAULT_DATA_DIR),
    )
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help='Force fetch registers even if no changes are detected',
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    data_dir.joinpath(STAC_SUBDIR).mkdir(parents=True, exist_ok=True)

    new_resources = {}
    new_registers = {}
    for register_url in args.register:
        register_name = get_register_name(register_url)
        register_fn = data_dir / f'register-{register_name}'
        if (process_result := process_register(
                register_url=register_url,
                register_fn=register_fn,
                data_dir=data_dir,
                fallback_sparql=args.fallback_sparql,
                force=args.force)) is not False:
            new_resources_register, new_register = process_result
            new_resources[register_url] = new_resources_register
            new_registers[register_fn] = new_register
            for resource in new_resources_register.values():
                resource['bblocks_register'] = register_url

    if not new_resources:
        sys.exit(1)

    with open(args.config_file) as f:
        existing_config = yaml.safe_load(f)

    all_resources = {
        k: v
        for k, v in existing_config.setdefault('resources', {}).items()
        if v.get('bblocks_register') not in new_resources
    }
    all_resources.update({k: v
                          for r in new_resources.values()
                          for k, v in r.items()})
    existing_config['resources'] = all_resources

    with open(args.config_file, 'w') as f:
        yaml.safe_dump(existing_config, f,
                       default_flow_style=False, sort_keys=False)

    for reg_path, reg_contents in new_registers.items():
        with open(reg_path, 'w') as f:
            json.dump(reg_contents, f, indent=2)


if __name__ == '__main__':
    _main()
