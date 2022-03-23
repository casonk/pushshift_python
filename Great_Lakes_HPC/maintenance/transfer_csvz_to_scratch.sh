#!/bin/bash
# Purpose: Copy csvz from Turbo to Scrath storage for faster I/O on cluster nodez.
# Author: Cason Konzer
# Last Modified: 3/20/2022
# ----------------------------------------

source="/nfs/turbo/flnt-mmani/Reddit_Communities/csvz/"
target="/scratch/mmani_root/mmani0/shared_data/csvz/"

echo "source := "$source
echo "target := "$target
echo

num_files=$(ls $source | wc -l)
echo "$num_files files to copy"
echo

rm -frd $target
mkdir $target
echo "target directory purged"
echo

i=0

csvz=($source*.csv*)
for file in "${csvz[@]}"

do 
	f="$(basename -- $file)"
	echo "copying $f"
	cp $file -fv -t $target
	echo

	let "i+=1" 
	echo "$i of $num_files files copied"
	echo
done