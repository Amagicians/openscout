#!/bin/bash
args=$*
./main.py $args &
./face.py $args &
./obj.py $args &
./hello.py $args
