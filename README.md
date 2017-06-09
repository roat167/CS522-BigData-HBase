Introduction	
	The application is written in Java using Hbase and MapReduce. It will :
		- create a sample table called "Employee" and its column families
		- add some data to table Employee
		- output a result that count current employee position			

note:
	src/ 		source code
	target/		classes files and other
	outputhbase/	contain output result 
	build.sh	script file for building classes and jar
	run.sh		script for processing log file and generate output files	

Instruction 
Assume that you have already have Hadoop installed and run on your machine.
Step 1: building jar
	Open Terminal from current directory
	Type : ./build.sh
Step 2: run the program
	Type : ./run.sh
	
NOTE: If you have connection refuse issue. 
	solution: Try to manual set configuration port ( conf.set("hbase.zookeeper.property.clientport", "2181"));	


		
