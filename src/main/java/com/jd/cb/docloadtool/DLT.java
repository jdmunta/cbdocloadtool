/**
 * 
 */
package com.jd.cb.docloadtool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * Document loading tool: Creation of a tool to insert Json documents into
 * couchbase using couchbase sdk based on an user given document size and count.
 * The tool can use any fixed schema. It can be asynchronous, multi-threaded,
 * batched to be faster and efficient. The interface may be create n number of
 * documents with distinct keys e.g. key1, key2 …, and arbitrary values. Looking
 * at Couchbase UI (localhost:8091) on the Bucket tab you should see the ops per
 * second be non-zero.
 * 
 * @author jagadeshmunta
 * 
 *
 */
public class DLT {

	static String CBSERVER = System.getProperty("cb.server", "localhost");
	static String USERNAME = System.getProperty("cb.username", "jagadesh");
	static String PASSWORD = System.getProperty("cb.userpwd", "Welcome123");
	static String BUCKET = System.getProperty("cb.bucket", "default");
	static long BATCHSIZE = Long.parseLong(System.getProperty("thread.batch.size", "100"));
	static int THREADCOUNT = Integer.parseInt(System.getProperty("thread.count", "10"));
	static int THREADPOOLSIZE = Integer.parseInt(System.getProperty("thread.pool.size", "10"));
	static int TIMEOUTMINS = Integer.parseInt(System.getProperty("thread.pool.wait.timeout.mins", "60"));
	static String DOCNAMEPREFIX = System.getProperty("json.doc.name.prefix", "sampledoc");
	static String DOCKEYPREFIX = System.getProperty("json.doc.key.prefix", "Key");
	static String DOCVALPREFIX = System.getProperty("json.doc.val.prefix", "JDVal");
	final static int JSONEXTRACHARCOUNT = 12;
	static CountDownLatch latch = new CountDownLatch(THREADCOUNT);

	static Logger logger = LoggerFactory.getLogger(DLT.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("*** Couchbase: Document Loading Tool ***\n");

		if (args.length < 1) {
			StringBuilder sbUsage = new StringBuilder();
			sbUsage.append("\nUsage: java com.jd.cb.docloadtool.DLT <size-in-bytes> <count>");
			sbUsage.append("\nSupported Falgs/system properties: ");
			sbUsage.append(
					"\n  -Dcb.username=jagadesh -Dcb.userpwd=Welcome123 -Dcb.bucket=default -Dthread.batch.size=100 -Dthread.pool.size=10 thread.pool.wait.timeout.mins=60 -Djson.doc.name.prefix=sampledoc -Djson.doc.key.prefix=Key -Djson.doc.val.prefix=JDVal");
			sbUsage.append("\nExample: To Load 10000 bytes json documents in 10000 count, use the below command.");
			sbUsage.append(
					"\njava -Djson.doc.name.prefix=sampledoc -jar target/docloadtool-0.0.1-SNAPSHOT-jar-with-dependencies.jar 10000 10000 ");
			System.out.println(sbUsage.toString());
			return;
		}
		long docExpSize = Long.parseLong(args[0]);
		long count = Long.parseLong(args[1]);
		loadSampleDocuments(docExpSize, count);
	}
	
	/**
	 * Load the documents with given docsize and count parameters
	 * 
	 * @param docExpSize
	 * @param count
	 * @return
	 */
	public static long loadSampleDocuments(long docExpSize, long count) {
	
	  return loadSampleDocuments(CBSERVER, USERNAME, PASSWORD, BUCKET, docExpSize, count, DOCNAMEPREFIX, DOCKEYPREFIX, DOCVALPREFIX, true);
	}
	
	/**
	 * 
	 * Load the documents with given below parameters
	 * 
	 * @param docExpSize
	 * @param count
	 * @param isParallelMode
	 * @return
	 */
	public static long loadSampleDocuments(long docExpSize, long count, boolean isParallelMode) {
	
	  return loadSampleDocuments(CBSERVER, USERNAME, PASSWORD, BUCKET, docExpSize, count, DOCNAMEPREFIX, DOCKEYPREFIX, DOCVALPREFIX, isParallelMode);
	}
	
	/**
	 * Load the documents with given below parameters
	 * 
	 * @param userName
	 * @param password
	 * @param docExpSize
	 * @param count
	 * @return
	 */
	public static long loadSampleDocuments(String userName, String password, long docExpSize, long count) {
	
	  return loadSampleDocuments(CBSERVER, userName, password, BUCKET, docExpSize, count, DOCNAMEPREFIX, DOCKEYPREFIX, DOCVALPREFIX, true);
	}
	
	/**
	 * Load the documents with given below parameters
	 * 
	 * @param userName
	 * @param password
	 * @param bucketName
	 * @param docExpSize
	 * @param count
	 * @return
	 */
	public static long loadSampleDocuments(String userName, String password, String bucketName, long docExpSize, long count) {
	
	  return loadSampleDocuments(CBSERVER, userName, password, bucketName, docExpSize, count, DOCNAMEPREFIX, DOCKEYPREFIX, DOCVALPREFIX, true);
	}
	
	/**
	 * Load the documents with given below parameters
	 * 
	 * @param cbServer
	 * @param userName
	 * @param password
	 * @param bucketName
	 * @param docExpSize
	 * @param count
	 * @param docNamePrefix
	 * @param docKeyPrefix
	 * @param docValuePrefix
	 * @return
	 */
	public static long loadSampleDocuments(String cbServer, String userName, String password, String bucketName, 
			long docExpSize, long count,String docNamePrefix, String docKeyPrefix, String docValuePrefix, boolean isParallelMode) {
		if (count <= 0) {
			logger.error("Enter the number of docs greater than 0.");
			return -1;
		}
		if (cbServer==null) { cbServer = CBSERVER; }
		if (userName==null) { userName = USERNAME; }
		if (password==null) { password = PASSWORD; }
		if (bucketName==null) { bucketName = BUCKET; }
		if (docNamePrefix==null) { docNamePrefix = DOCNAMEPREFIX; }
		if (docKeyPrefix==null) { docKeyPrefix = DOCKEYPREFIX; }
		if (docValuePrefix==null) { docValuePrefix = DOCVALPREFIX; }
		
		
		
		// Key<num> + Val prefix<count> + extra characters in json
		int minSize = (docKeyPrefix + count + docValuePrefix + count + "-").length() + JSONEXTRACHARCOUNT;
		if (docExpSize < minSize) {
			logger.error("Enter the appropriate json doc size. It should be greater than " + minSize
					+ " so that appropriate key and value prefix can be added.");
			JsonObject jo = JsonObject.create();
			jo.put(docKeyPrefix + count, docValuePrefix + count);
			logger.error("Example:" + jo.toString());
			return -1;
		}

		// Connect to the resources
		logger.info("Connecting to the Couchbase server at " +cbServer +" with user: "+userName + ", Bucket: "+bucketName);
		Cluster cbCluster = CouchbaseCluster.create(cbServer);
		cbCluster.authenticate(userName, password);
		Bucket bucket = cbCluster.openBucket(bucketName);

		System.out.println("=======================================================================");
		//System.out.println("CB server: " + cbServer +", Bucket: "+bucketName + ", Username: " + userName );
		System.out.println("Input document size: " + docExpSize + ", number of documents: " + count);
		System.out.println("Json document name prefix: " + docNamePrefix + ", Key prefix: " + docKeyPrefix
				+ ", Value prefix: " + docValuePrefix);
		logger.debug("Threads count:" + THREADCOUNT);
		logger.debug("Thread pool size:" + THREADPOOLSIZE);
		System.out.println("Loading...please wait.");

		long startTime = 0;
		long endTime = 0;
		long total = 0;
		startTime = System.nanoTime();	
		if (isParallelMode) {
			total = loadDocsBatchInParallel(bucket, docExpSize, count);
		} else {
			total = loadDocsInSeq(bucket, docExpSize, count);
		}
		endTime = System.nanoTime();
		System.out.println("Completed.");
		System.out.println("Total loading time taken: " + (endTime - startTime) / 1e6 + " millisecs");
		System.out.println("=======================================================================");
		
		// Close the resources
		bucket.close();
		cbCluster.disconnect();
		
		return total;
	}

	/*
	 * Load the docs loading in batches with parallel threads
	 */
	public static long loadDocsBatchInParallel(Bucket bucket, long docSize, long count) {

		// Connect to CB bucket.
		/*
		 * Cluster cbCluster = CouchbaseCluster.create(CBSERVER);
		 * cbCluster.authenticate(USERNAME, PASSWORD); Bucket bucket =
		 * cbCluster.openBucket(BUCKET);
		 */

		// Generate and load the given count of documents of the given size.
		long docNum = 1; // start prefix. TBD: can be parameterized.
		if (count < THREADPOOLSIZE) {
			THREADPOOLSIZE = (int)count;
		} else if (count < BATCHSIZE) {
			BATCHSIZE = (int)count;
		}
		ExecutorService threadPool = Executors.newFixedThreadPool(THREADPOOLSIZE);

		DocLoadBatchWorkerThread thread = null;
		long batchcount = count / BATCHSIZE;
		long tindex = 1;
		for (int bindex = 0; bindex < batchcount; bindex++) {

			docNum = (bindex * BATCHSIZE) + 1;
			thread = new DocLoadBatchWorkerThread(bucket, DOCNAMEPREFIX, docSize, BATCHSIZE, docNum);
			threadPool.execute(thread);

			// Wait for thread count to finish
			if (tindex % THREADCOUNT == 0) {
				try {
					logger.debug("Waiting for threads to complete..." + tindex);
					latch.await();
					latch = new CountDownLatch(THREADCOUNT);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			tindex++;

		}

		long rbatch = count % BATCHSIZE;
		if (rbatch > 0) {
			thread = new DocLoadBatchWorkerThread(bucket, DOCNAMEPREFIX, docSize, rbatch, docNum);
			threadPool.execute(thread);
		}

		threadPool.shutdown();
		try {
			threadPool.awaitTermination(TIMEOUTMINS, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long total =docNum + BATCHSIZE - 1;
		logger.info("Total documents inserted: " + total);
		return total;
	}



	/*
	 * Load the docs sequentially
	 */
	public static long loadDocsInSeq(Bucket bucket, long docSize, long count) {

		long docNum = 1; //
		// Insert count number of documents
		for (; docNum <= count; docNum++) {

			// Generate the document with unique key and value and also to match the given
			// size

			String key = DOCKEYPREFIX + docNum;
			String valPrefix = DOCVALPREFIX + docNum + "-";
			JsonObject jo = JsonObject.create();
			jo.put(key, generateArbitraryData((int)docSize - key.length() - valPrefix.length() - JSONEXTRACHARCOUNT, true,
					true, valPrefix)); // JSONEXTRACHARCOUNT characters are due to json { quotes etc.
			String docName = DOCNAMEPREFIX + docNum;
			logger.debug("Inserting doc name: " + docName);
			logger.debug(jo.toString());
			bucket.upsert(JsonDocument.create(docName, jo));

		}
		long total = (docNum - 1);
		logger.info("Total documents inserted: " + total );
		return total;

	}


	/*
	 * Thread to generate a document and load into the given bucket
	 */
	static class DocLoadBatchWorkerThread implements Runnable {

		long docSize = 0;
		String docNamePrefix = null;
		Bucket bucket = null;
		long count = 0;
		long startKeySuffix = 0;

		public DocLoadBatchWorkerThread(Bucket bucket, String docNamePrefix, long docSize, long count,
				long startKeySuffix) {
			this.bucket = bucket;
			this.docNamePrefix = docNamePrefix;
			this.docSize = docSize;
			this.count = count;
			this.startKeySuffix = startKeySuffix;
		}

		public void run() {
			// Generate the document with unique key and value and also to match the given
			// size
			storeData();

		}

		public synchronized void storeData() {

			long endindex = startKeySuffix + count;
			for (long index = startKeySuffix; index < endindex; index++) {
				String key = DOCKEYPREFIX + index;
				String valPrefix = DOCVALPREFIX + index + "-";
				JsonObject jo = JsonObject.create();
				jo.put(key, generateArbitraryData((int)docSize - key.length() - valPrefix.length() - JSONEXTRACHARCOUNT,
						true, true, valPrefix)); // JSONEXTRACHARCOUNT characters are due to json { quotes etc.
				String docName = docNamePrefix + index;
				logger.debug("Inserting doc name: " + docName);
				logger.debug(jo.toString());
				bucket.upsert(JsonDocument.create(docName, jo));
			}
			latch.countDown();
		}
	}

	// Generate Data string
	static String generateArbitraryData(int size, boolean useAlphabets, boolean useNums, String prefix) {
		if (size <= 0) {
			return prefix;
		}
		String rString = null;
		try {
			rString = RandomStringUtils.random(size - 1, useAlphabets, useNums);
		} catch (IllegalArgumentException iae) {
			logger.error(
					"Can't generate the arbitrary value. It might be that the given document size is too low to have the right key name and value prefix. Please increase it.");
		}
		if (prefix != null) {
			rString = prefix + rString;
		}
		return rString;

	}

}
