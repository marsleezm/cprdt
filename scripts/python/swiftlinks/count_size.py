#!/usr/bin/env python
import sys

f = open(sys.argv[1], "r")

tot_size = 0
num_replies = 0

for line in f:
	result = line.rstrip().split(',')
	
	if len(result) >= 3 and result[2] == 'METADATA_FetchObjectVersionReply':
		num_replies += 1
		tot_size += int(result[3])

print("Avg size: "+ str(float(tot_size) / num_replies))
print("Tot size: "+ str(tot_size))
