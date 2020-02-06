# import-bitergia-indexes
Import Bitergia ElasticSearch dumps into ES database (prefix them with `bitergia-`).

# Usage

- Start local ES server via: `./es_local_docker.sh`.
- To import data form Bitergia exported JSON dump files do: `[DEBUG=1] [NO_LOG=1] [ES_URL=...] [MAX_TOKEN=2] [MAX_LINE=2048] [ALLOW_MAP_FAIL=1] [ALLOW_DATA_FAIL=1] ./import-bitergia-indexes file1.json file2.json ...`

# Important

When you unzip data files you should have 2 distinct types of JSON files:

- Actual data files (without `mapping` in their name).
- Mapping files (containing `mapping` in their name).

For each data file, call it `data.json` you need to rename its mapping to be `data.json.map`, so for each data JSON there should be a file with the same name + `.map` prefix for that JSON.

If there is no mapping for a given JSON file, first document inserted from data file will determine document format, so all other documents from this file must have the same format.

It means that if some document is using `1` for float value and there is no mapping, then ES will assign type `int` to that column, so if so latter document uses `1.5` - it will cause error due to attempt to insert `float` value into an `int` column.

This is why each data file should have `.map` file to specify exact types for all its documents columns.

# Correlate

Bitergia export uses the following pattern for their files: data file in `directory/fn_data.json`, map file `directory/fn_mapping.json`.

To update mapping file to `directory/fn_data.json.map` run `./correlate_indices.sh ./directory`.
