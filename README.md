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

## Lab 03

### Objective
Write Hive queries in HQL to solve simple problems with Hadoop and Hive.

*These are the same problems than in Lab 02.*

**Q1**: Find the number of first names for each origin.

**Q2**: Find the number of first names for each number of origins.

**Q3**: Find the proportion for each gender.

### Instructions
On the Hadoop server, run the following command:
```
hive
```

Type in the HQL queries from lab2bis.hql as needed.

## Lab 04

### Objective
Write a simplistic social network app accessing an HBase database.*

Write a REPL that allows interaction with this database.

### Information
The Main.java file contains the code for the REPL.

The SocialNetwork.java file contains the code to interact with HBase.

The Person.java file defines a class used as a container by the rest of the app.

### Instructions
Build a .jar file with the Main.java, Person.java, and SocialNetwork.java files. A pom.xml file is provided to use Maven.

On the Hadoop server run the following command:
```
hadoop jar <jar file> Main
```

Once the program is running, you can use the REPL. The following commands are available:

To add a person to the database. First name and BFF are mandatory, the rest is not.
```
add person firstname=<string>
           bff=<string>
           [lastname=<string>]
           [birthdate=<string>]
           [city=<string>]
           [email=<string>]
           [otherfriends=<string>,<string>,<string>]
```
To remove a person from the database.
```
delete person firstname=<string>
```
To display a person's information.
```
show person firstname=<string>
```
To list all people in the database.
```
show all
```
To check the consistency of the database. Here this means checking that all names listed as BFF's correspond to people present in the database.
```
check consistency
```
To exit the REPL and the program itself.
```
exit
```
