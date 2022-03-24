#!/bin/bash
start_time=$(date +%s)
[ -e /tmp/fd1 ] || mkfifo /tmp/fd1
exec 3<>/tmp/fd1
rm -rf /tmp/fd1
# 同时执行 2 个线程
for ((i = 1; i <= 2; i++)); do
  echo >&3
done

for ((i = 1; i <= 20; i++)); do
  read -u3
  {
    echo "ROUND $i"
    sudo make project2c > ./out/ALL2c-$i.txt;
    echo >&3
  } &
done
wait

stop_time=$(date +%s)

echo "TIME:$(expr $stop_time - $start_time)"
exec 3<&-
exec 3>&-