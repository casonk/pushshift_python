#!/bin/bash
# Purpose: Decompress all tarballz in a folder to a folder of csvz.
# Author: Cason Konzer
# Last Modified: 3/20/2022
# ----------------------------------------

source="/nfs/turbo/flnt-mmani/Reddit_Communities/csvz/" 
target="/nfs/turbo/flnt-mmani/Reddit_Communities/tarballz/"

echo "source := "$source
echo "target := "$target
echo

num_files=$(ls $source | wc -l)
echo "$num_files files to tar"
echo

i=0

csvz=($source*.csv*)
for file in "${csvz[@]}"

do 
	f="$(basename -- $file)"
	echo "compressing $f"
	tar -vcz $target $file
	echo

	let "i+=1" 
	echo "$i of $num_files files tared"
	echo
done