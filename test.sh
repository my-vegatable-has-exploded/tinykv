#! /bin/bash
for ((i=1;i<=20;i++));
do
	echo "ROUND $i";
	make project2b > ./out/out-$i.TestConcurrent;
done