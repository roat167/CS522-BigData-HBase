#!/bin/sh

#cleaning
hadoop fs -rm -r -f /user/cloudera/outputhbase
#

#submitting JAR file
#hadoop jar target/hbaseproject-0.0.1-SNAPSHOT.jar hbaseproject.Application
#java -jar target/hbaseproject-0.0.1-SNAPSHOT.jar

spark-submit --class hbaseproject.Application target/hbaseproject-0.0.1-SNAPSHOT.jar

#outputhbase directory check
if [ -d "outputhbase" ]; then
	rm -r -f outputhbase
fi
#
mkdir outputhbase
#import outputhbase
hadoop fs -get /user/cloudera/outputhbase/* ./outputhbase




