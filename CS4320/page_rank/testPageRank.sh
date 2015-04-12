#! /bin/bash

testdir="tests"
input="input"
output="stage9/output.txt"
expectedDir="expected"
for f in "$testdir"/* 
do
	echo "$f"
	f=
	expected="$expectedDir"/"$f"
	rm "$input"/* 
	make clean
	cp "$f" "$input"/
	make &>log.txt		
	echo $expected 
	paste -d ; output "$expected" 
done	
