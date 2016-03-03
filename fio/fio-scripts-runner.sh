#!/bin/sh
sudo bash RandomRead.sh > ~/fio-outputs/RandomRead.out 
sudo bash RandomWrite.sh > ~/fio-outputs/RandomWrite.out 
sudo bash RandomR3W1.sh > ~/fio-outputs/RandomR3W1.out 
sudo bash RandomR2W2.sh > ~/fio-outputs/RandomR2W2.out 
sudo bash RandomR1W3.sh > ~/fio-outputs/RandomR1W3.out 
sudo bash SeqRead.sh > ~/fio-outputs/SeqRead.out 
sudo bash SeqWrite.sh > ~/fio-outputs/SeqWrite.out 
sudo bash SeqR3W1.sh > ~/fio-outputs/SeqR3W1.out 
sudo bash SeqR2W2.sh > ~/fio-outputs/SeqR2W2.out 
sudo bash SeqR1W3.sh > ~/fio-outputs/SeqR1W3.out
