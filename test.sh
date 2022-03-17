#! /bin/bash
for ((i=1;i<=100;i++));
do
	echo "ROUND $i";
	make project2b > ./out/ALL-$i.out;
done
