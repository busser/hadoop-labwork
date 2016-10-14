import sys

# A Map() function compatible with Hadoop. The goal is to use MapReduce to establish a word count.
def myMapFunction():
	for line in sys.stdin: # Input is treated line by line
		for word in line.strip().split(): # Each line is split into words
			print(word + " 1") # A (word, 1) tuple is output for each word

myMapFunction()
