#!/bin/bash
gcc -o dbio dbase.c utils/logger.* utils/xmm.c utils/md5.c -Iutils -DHAVE_MMAP -D_DEBUG_DBIO
[ "$#" -gt 0 ] && n=$1 || n=0;
[ "$n" -lt 1 ] && (echo "Usage:$0 N(threads)";exit;)
if [ $n -gt 0 ]
then
    i=0;
    while [ $i -lt $n ];
    do
        eval "./dbio `expr $i \* 1000000` 1000000";
        ((i++));
    done
fi
