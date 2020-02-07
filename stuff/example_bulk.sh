#!/bin/bash
curl -XPOST "http://localhost:19200/bitergia-github_oci_180322/_bulk?refresh=false" -H 'Content-Type: application/x-ndjson' --data-binary '
{"index": {"_index":"tst"}}
{"category":"issue","metadata__timestamp":"2018-06-29T13:38:43.809941+00:00"}
{"index": {"_index":"tst"}}
{"category":"issue","metadata__timestamp":"2018-05-29T13:38:43.809941+00:00"}
'
