# cbdocloadtool
Couchbase document load tool


Steps:
1) Build the jar file
mvn clean compile assembly:single

2) Run the tool for usage.

$ java -jar target/docloadtool-0.0.1-SNAPSHOT-jar-with-dependencies.jar
*** Couchbase: Document Loading Tool ***


Usage: java com.jd.cb.docloadtool.DLT <size-in-bytes> <count>
Supported Falgs/system properties: 
  -Dcb.username=jagadesh -Dcb.userpwd=Welcome123 -Dcb.bucket=default -Dthread.batch.size=100 -Dthread.pool.size=10 thread.pool.wait.timeout.mins=60 -Djson.doc.name.prefix=sampledoc -Djson.doc.key.prefix=Key -Djson.doc.val.prefix=JDVal
Example: To Load 10000 bytes json documents in 10000 count, use the below command.
java -Djson.doc.name.prefix=sampledoc -jar target/docloadtool-0.0.1-SNAPSHOT-jar-with-dependencies.jar 10000 10000 



