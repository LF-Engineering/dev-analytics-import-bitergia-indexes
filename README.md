# import-bitergia-indexes
Import Bitergia ElasticSearch dumps into ES database (prefix them with `bitergia-`).

# Usage

- Start local ES server via: `./es_local_docker.sh`.
- To import data form Bitergia exported JSON dump files do: `[DEBUG=1] ES_URL=... ./import-bitergia-indexes file1.json file2.json ...`
