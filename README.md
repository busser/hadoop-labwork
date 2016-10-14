# Big Data with Hadoop - Labwork

## Lab 01

### Objective
Implement a simple word count with Map() and Reduce() functions compatible with Hadoop.

### Instructions
Run the following command:
```
python map.py < data.in | python sort.py | python reduce.py > data.out
```

## Lab 02

### Objective
Write MapReduce classes in Java to solve simple problems with Hadoop.

**Q1**: Find the number of first names for each origin.

**Q2**: Find the number of first names for each number of origins.

**Q3**: Find the proportion for each gender.

### Instructions
Build a .jar file with the Q1.java, Q2.java, Q3.java files. A pom.xml is provided to use Maven.

On the Hadoop server, run the following command:
```
hadoop jar <jar file> <Q1, Q2, or Q3> <input file or folder> <output folder>
```
