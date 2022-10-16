#!/bin/bash
# Purpose: Copy from Scrath to Turbo storage for archive.
# Author: Cason Konzer
# Last Modified: 7/17/2022
# ----------------------------------------

source="/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/IDL/"
target="/home/casonk/path/flnt-mmani/Reddit_Communities/push_file/IDL/"

echo "source := "$source
echo "target := "$target
echo

num_files=$(ls $source | wc -l)
echo "$num_files files/directories to copy"
echo

rm -frd $target
mkdir $target
echo "target directory purged"
echo

for file in $source

do 
	f="$(basename -- $file)"
	echo "copying $f"
	cp $file -frv -t $target
	echo

done
