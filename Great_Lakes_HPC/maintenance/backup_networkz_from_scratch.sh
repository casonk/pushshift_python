#!/bin/bash
# Purpose: Copy csv_labelz from Turbo to Scrath storage for faster I/O on cluster nodez.
# Author: Cason Konzer
# Last Modified: 3/20/2022
# ----------------------------------------

source_csvz="/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/"
target_csvz="/nfs/turbo/flnt-mmani/Reddit_Communities/buffer/csv_networkz/"

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

i=0

csvz=($source_csvz*.csv*)
for file in "${csvz[@]}"

do 
	f="$(basename -- $file)"
	echo "copying $f"
	cp $file -fv -t $target_csvz
	echo

	let "i+=1" 
	echo "$i of $num_files files copied"
	echo
done

source_pklz="/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/"
target_pklz="/nfs/turbo/flnt-mmani/Reddit_Communities/buffer/pkl_networkz/"

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

j=0

pklz=($source_pklz*.pkl*)
for file in "${pklz[@]}"

do 
	f="$(basename -- $file)"
	echo "copying $f"
	cp $file -fv -t $target_pklz
	echo

	let "j+=1" 
	echo "$j of $num_files files copied"
	echo
done