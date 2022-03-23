#!/bin/bash
# Purpose: Decompress all tarballz in a folder to a folder of csvz.
# Author: Cason Konzer
# Last Modified: 3/20/2022
# ----------------------------------------

source="/nfs/turbo/flnt-mmani/Reddit_Communities/tarballz/"
target="/nfs/turbo/flnt-mmani/Reddit_Communities/csvz/"

echo "source := "$source
echo "target := "$target
echo

num_files=$(ls $source | wc -l)
echo "$num_files files to untar"
echo

i=0

tarballz=($source*.csv*)
for file in "${tarballz[@]}"

do 
	f="$(basename -- $file)"
	echo "decompressing $f"
	tar -fvxz $file -C $target
	echo

	let "i+=1" 
	echo "$i of $num_files files untared"
	echo
done