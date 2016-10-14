import sys

# This code acts between the Map() and Reduce() functions. It sorts the tuples output by Map() by keys.
for x in sorted([tuple(line.strip().split()) for line in sys.stdin], key = lambda x: x[0]):
	print(*x)
