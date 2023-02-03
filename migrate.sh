#!/bin/bash
set -e

rm -rf $PWD/log
mkdir -p $PWD/log

for i in $(ls 0*.sh); do
	now=$(date "+%Y-%m-%d %T")
	echo "Starting ${i} at ${now}"
	$PWD/${i}
done

now=$(date "+%Y-%m-%d %T")
echo "INFO: Completed at ${now}"
