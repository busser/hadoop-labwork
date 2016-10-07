import sys

# A Reduce() function compatible with Hadoop. The goal is to use MapReduce to establish a word count.
def myReduceFunction():
	word_previous = ""
	count_total = 0

	for line in sys.stdin: # Input is treated line by line. The lines are sorted
		word, count = line.strip().split() # Each line is split into a word and a count
		count = int(count) # The count is converted from a string to an integer
		if word != word_previous: # If the word just read is different than the previous one
			if word_previous != "": # If this isn't the first word we treat
				print(word_previous, count_total) # Output the previous word and its total count
			word_previous = word # Save current word for future comparison
			count_total = count # Start count for this word at value in first tuple with that word as a key
		else: # If the word just read is the same as the previous one
			count_total = count_total + count # Add count from new tuple to total word count
	print(word_previous, count_total) # Print last word treated and its word count

myReduceFunction()
