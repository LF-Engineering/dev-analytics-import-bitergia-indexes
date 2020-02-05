#!/bin/bash
clear
make && DEBUG=1 NO_LOG='' ES_URL='http://localhost:19200' ./import-bitergia-indexes oci/*.json
