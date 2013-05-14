#!/bin/bash
gcc -o dbase dbase.c utils/xmm.c utils/md5.c utils/logger.c -Iutils -DHAVE_MMAP -D_DEBUG_DBASE 

[ "$#" -gt 0 ] && n=$1 || n=0;
[ "$n" -lt 1 ] && (echo "Usage:$0 N(threads)";exit;)
if [ $n -gt 0 ]
then
    i=0;
    while [ $i -lt $n ];
    do
        eval "./dbase -h 127.0.0.1 -p2481 -n 10000000 -f `expr $i \* 10000000` -d";
        ((i++));
    done
fi
