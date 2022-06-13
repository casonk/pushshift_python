#!/bin/bash
# Purpose: Copy csvz from Turbo to Scrath storage for faster I/O on cluster nodez.
# Author: Cason Konzer
# Last Modified: 6/13/2022
# ----------------------------------------

source="/scratch/mmani_root/mmani0/shared_data/hot/csvz/"
target="/nfs/turbo/flnt-mmani/Reddit_Communities/csvz/"

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

# ----------------------------------------

source="/scratch/mmani_root/mmani0/shared_data/hot/csv_ekoz/"
target="/nfs/turbo/flnt-mmani/Reddit_Communities/csv_ekoz/"

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

# ----------------------------------------

source="/scratch/mmani_root/mmani0/shared_data/hot/csv_labelz/"
target="/nfs/turbo/flnt-mmani/Reddit_Communities/csv_labelz/"

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

# ----------------------------------------

source="/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/"
target="/nfs/turbo/flnt-mmani/Reddit_Communities/csv_networkz/"

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

# ----------------------------------------

source="/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/"
target="/nfs/turbo/flnt-mmani/Reddit_Communities/pkl_networkz/"

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

# ----------------------------------------