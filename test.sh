#! /bin/bash
project="project$1"
output="$1"
for ((i=1;i<=($2);i++));
do
	echo "ROUND $i";
	make $project > ./out/$output-$i.out;
	sleep 300s
done
