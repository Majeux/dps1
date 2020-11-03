#!/usr/bin/python3
import fileinput

start = -1.0
for line in fileinput.input():
    if start == -1.0:
        start = float(line)

    print(float(line)-start)
