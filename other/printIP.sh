#!/bin/bash
while read line
do
 echo `cat /etc/hosts|grep $line|awk '{print $1}'`
done < hostsWithCallTraces 
