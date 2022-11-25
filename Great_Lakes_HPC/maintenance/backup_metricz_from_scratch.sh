#!/bin/bash
# Purpose: Copy from Scrath to Turbo storage for archive.
# Author: Cason Konzer
# Last Modified: 3/20/2022
# ----------------------------------------

source_csvz="/scratch/mmani_root/mmani0/shared_data/hot/csv_ekoz/"
target_csvz="/nfs/turbo/flnt-mmani/Reddit_Communities/csv_ekoz/"

echo "source_csvz := "$source_csvz
echo "target_csvz := "$target_csvz
echo

num_files=$(ls $source_csvz | wc -l)
echo "$num_files files to copy"
echo

rm -frd $target_csvz
mkdir $target_csvz
echo "target_csvz directory purged"
echo


csvz=($source_csvz*)
for file in "${csvz[@]}"

do 
	f="$(basename -- $file)"
	echo "copying $f"
	cp $file -frv -t $target_csvz
	echo
done