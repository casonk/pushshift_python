#!/bin/bash
# Purpose: Copy csv_labelz from Turbo to Scrath storage for faster I/O on cluster nodez.
# Author: Cason Konzer
# Last Modified: 3/20/2022
# ----------------------------------------

source_csvz="/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/"
target_csvz="/nfs/turbo/flnt-mmani/Reddit_Communities/csv_networkz/"

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

source_pklz="/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/"
target_pklz="/nfs/turbo/flnt-mmani/Reddit_Communities/pkl_networkz/"

echo "source_pklz := "$source_pklz
echo "target_pklz := "$target_pklz
echo

num_files=$(ls $source_pklz | wc -l)
echo "$num_files files to copy"
echo

rm -frd $target_pklz
mkdir $target_pklz
echo "target directory purged"
echo

pklz=($source_pklz*)
for file in "${pklz[@]}"

do 
	f="$(basename -- $file)"
	echo "copying $f"
	cp $file -frv -t $target_pklz
	echo
done