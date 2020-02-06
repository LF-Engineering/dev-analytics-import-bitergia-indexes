#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: please provide a directory name as an argument"
  exit 1
fi
for f in `find $1/ -type f -iname "*_data.json"`
do
  m="${f/_data/_mapping}"
  if ( [ -f "$f" ] && [ -f "$m" ] )
  then
    mv "$m" "${f}.map"
  else
    echo "not found $f --> $m"
  fi
done
