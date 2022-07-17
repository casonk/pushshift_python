#!/bin/bash
# Purpose: Copy csvz from Turbo to Scrath storage for faster I/O on cluster nodez.
# Author: Cason Konzer
# Last Modified: 7/26/2022
# ----------------------------------------

source="/home/casonk/path/flnt-mmani/Reddit_Communities/push_file/RC/"
target="/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/RC/"

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

zstz=($source*.zst*)
for file in "${zstz[@]}"

do 
	f="$(basename -- $file)"
	echo "copying $f"
	cp $file -fv -t $target
	echo

	let "i+=1" 
	echo "$i of $num_files files copied"
	echo
done

source="/home/casonk/path/flnt-mmani/Reddit_Communities/push_file/RS/"
target="/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/RS/"

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

zstz=($source*.zst*)
for file in "${zstz[@]}"

do 
	f="$(basename -- $file)"
	echo "copying $f"
	cp $file -fv -t $target
	echo

	let "i+=1" 
	echo "$i of $num_files files copied"
	echo
done