#!/bin/bash
x=0;
while [ $x -lt 16 ]
do
    mask="234.8.8.$x"
    wget -d -O /dev/null "http://127.0.0.1:2480/q" --post-data="op=6&diskid=0&mask=$mask";
    ((x++));

done;
