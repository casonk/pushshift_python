#!/bin/bash
# Purpose: Copy csvz from Turbo to Scrath storage for faster I/O on cluster nodez.
# Author: Cason Konzer
# Last Modified: 7/17/2022
# ----------------------------------------

source="/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/IDL/"
target="/home/casonk/path/flnt-mmani/Reddit_Communities/push_file/IDL/"

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
	cp $file -frv -t $target
	echo

	let "i+=1" 
	echo "$i of $num_files files copied"
	echo
done

pklz=($source*.pkl*)
for file in "${pklz[@]}"

do 
	f="$(basename -- $file)"
	echo "copying $f"
	cp $file -frv -t $target
	echo

	let "i+=1" 
	echo "$i of $num_files files copied"
	echo
done