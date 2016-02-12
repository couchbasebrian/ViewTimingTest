// ViewTimingTestRoundtrip
// Brian Williams
//
// Created: June 11, 2015
// Updated: February 12, 2016
//
// Developed with Couchbase Java SDK version 2.1.3 available from:
// http://packages.couchbase.com/clients/java/2.1.3/Couchbase-Java-Client-2.1.3.zip
//
// Given a cluster, connect
// Create a bucket
// Create a design document for the bucket
// Create a view in the design document
// Then
// Insert data into the bucket
// Query the view for some data
// Query the bucket for some of the items
// Compare getting items individually vs. getting same items from a specific view

// Note:  This program does create a bucket, a design document, and a view, so you do need some
// sufficient RAM in the server quota to accomodate this new bucket's bucket quota for RAM.
// Otherwise you could get a "RAM quota specified is too large to be provisioned into this cluster" 
// error message.  Also the program does not currently delete the bucket after completion.

// November/December Update:
// This program, ViewTimingTestRoundtrip, has forked from the original program, ViewTimingTest,
// whose intention which was to perform timing of view operations.
// This update adds a more advanced View function, which inserts a timestamp into the document.
// It also adds code to analyze the View results, and look inside the docs, Extract the timestamp, and compare it to the current time.
// The goal of this is to measure the amount of time from the creation time, to when the view emitted the result, to when it was
// actually gotten by a client.

// The next iteration of this would be to first set up the bucket, populate the bucket, create the design doc, and create the view,
// and then to have two threads which would operate continuously.  One thread would frequently update the documents ( perhaps one 
// document would suffice ) and another thread would run queries and check the time stamps, to report on the latencies, from 
// updating the doc, to View engine emit, to actual reception by client.  And then report on the differences in real time.
// With a gui you could plot this on a graph.
//
// For reference, the documents placed in the bucket look like this:
//
// {
//  "name": "testDocument",
//  "creationDate": 1450738843252,
//  "serialNumber": 0
// }
//
// The output from the view query ( each row ) looks like this
//
// {   "id":"testDocument0",
//    "key":"testDocument0",
//  "value":{
//           "name":"testDocument",
//           "creationDate":1450738843252,
//           "serialNumber":0,
//           "viewDateNow":1450738848237
//          }
// }
//



package com.couchbase.support;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.View;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.couchbase.client.java.bucket.BucketManager;
import com.google.gson.Gson;
import com.google.gson.JsonParser;

public class ViewTimingTestRoundtrip {

	static final int SCREENCOLUMNS = 100;	// adjust to fit your terminal
	
	public static void main(String[] args) {

		long programAbsoluteStart = System.currentTimeMillis();
		
		Gson gson = new Gson();
		
		String HOSTNAME           = "10.111.90.101";       // Put your cluster IP address here
		String USERNAME           = "Administrator";
		String PASSWORD           = "couchbase";        // Put your password here
		String BUCKETNAMEPREFIX   = "testBucket";		// The actual bucket name will be, say, testBucket585
		int    MAXBUCKETNUMBER    = 1000;        
		
		String DESIGNDOCUMENTNAME = "dd1";
		String VIEWNAME           = "vn1";

		String MAPFUNCTION        = "function (doc, meta) { doc.viewDateNow = Date.now(); emit(meta.id, doc); }";
		
		// You can use this to emit fewer results
		// String MAPFUNCTION        = "function (doc, meta) { if ((doc.serialNumber % 100) == 0) { doc.viewDateNow = Date.now(); emit(meta.id, doc); } } ";
		
		int    NUMDOCUMENTS       = 1000;				// The number of documents to create in the bucket
		
		// Connect to the cluster
		CBConnectTimer ct = new CBConnectTimer(HOSTNAME);
		runATimingClass(ct);
		CouchbaseCluster cluster = ct.getCluster();
		long timeToConnect = ct.getElapsedTime();
		
		// Come up with a bucket name and create a bucket
		int randomIdentifier = (int) (Math.random() * MAXBUCKETNUMBER);
		String newBucketName = BUCKETNAMEPREFIX + randomIdentifier;
		printCenteredBanner("The Bucket name for this test is " + newBucketName);
		CBCreateBucketTimer cbt = new CBCreateBucketTimer(cluster, newBucketName, USERNAME, PASSWORD);
		runATimingClass(cbt);
		long timeToCreateBucket = cbt.getElapsedTime();

		// Open the new bucket
		CBOpenBucketTimer bt = new CBOpenBucketTimer(cluster, newBucketName);
		runATimingClass(bt);
		Bucket bucket = bt.getBucket();
		long timeToOpenBucket = bt.getElapsedTime();
		
		// Create a Prod design document and View on the Bucket
		CBCreateDesignDocumentTimer cddt = new CBCreateDesignDocumentTimer(bucket, DESIGNDOCUMENTNAME, VIEWNAME, MAPFUNCTION);
		runATimingClass(cddt);
		long timeToCreateDesignDocument = cddt.getElapsedTime();
				
		// Insert data into the bucket
		CBPopulateBucketTimer fbt = new CBPopulateBucketTimer(bucket, NUMDOCUMENTS);
		runATimingClass(fbt);
		long timeToPopulateBucket = fbt.getElapsedTime();
		
		// Get some specific items
		String[] listOfDocumentIds = {
				"testDocument0",
				"testDocument100",
				"testDocument200",
				"testDocument300",
				"testDocument400",
				"testDocument500",
				"testDocument600",
				"testDocument700",
				"testDocument800",
				"testDocument900"
		};
		
		CBSingleGetItemsTimer sgit = new CBSingleGetItemsTimer(bucket, listOfDocumentIds);
		runATimingClass(sgit);
		logMessage("The success count was " + sgit.getSuccessCount());
		long timeToSingleGetItems = sgit.getElapsedTime();

		// Query the view with Stale = False
		CBFullViewQueryTimer fvqt2 = new CBFullViewQueryTimer(bucket, DESIGNDOCUMENTNAME, VIEWNAME, Stale.FALSE);
		runATimingClass(fvqt2);
		logMessage("The total results count was " + fvqt2.getTotalResults());
		long timeToFullViewQuery = fvqt2.getElapsedTime();
				
		boolean performPollingTest = true;
		long timeToGetCompleteResultSet = 0;  
		
		if (performPollingTest) {
			logMessage("Performing polling test");	
			// Query the view with Stale = Update After which is the default
			int iterationCount         = 0;
			boolean gotExpectedResults = false;
			CBFullViewQueryTimer fvqt1 = null;
			int resultsSeen            = 0;
			int expectedResults        = NUMDOCUMENTS;
					
			logMessage("I am expecting " + expectedResults + " results.  Pausing one minute.");
			
			// one minute pause
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			// Keep iterating until you get the expected results
			while (gotExpectedResults == false) {
				fvqt1 = new CBFullViewQueryTimer(bucket, DESIGNDOCUMENTNAME, VIEWNAME, Stale.FALSE);
				runATimingClass(fvqt1);
				resultsSeen = fvqt1.getTotalResults();  // Look at the count of results that were gotten
				if (resultsSeen == expectedResults) {
					gotExpectedResults = true;
					timeToGetCompleteResultSet = fvqt1.getElapsedTime();
				} 
				logMessage("Iteration " + iterationCount + ": The total results count was " + fvqt1.getTotalResults());
				iterationCount++;
				
				// Analyze these results
				analyzeTimestampsInResults(gson, fvqt1.getViewResult());

			}
			
			logMessage("Got the expected results.  Done with polling test.");

		}
		
		// Print Results
		logMessage("Time to connect:                                         " + timeToConnect + " ms.");
		logMessage("Time to create bucket:                                   " + timeToCreateBucket + " ms.");
		logMessage("Time to open bucket:                                     " + timeToOpenBucket + " ms.");
		logMessage("Time to create design document:                          " + timeToCreateDesignDocument + " ms.");
		logMessage("Time to populate bucket:                                 " + timeToPopulateBucket + " ms.");
		logMessage("Time to get single items:                                " + timeToSingleGetItems + " ms.");
		logMessage("Time to do full view query (with stale = false):         " + timeToFullViewQuery + " ms.");
		logMessage("Time to do full view query (with stale = update_after):  " + timeToGetCompleteResultSet + " ms.");

		// Clean up		
		bucket.close();
		cluster.disconnect();
		
		long programAbsoluteFinish = System.currentTimeMillis();

		logMessage("Total run time of this program:                          " + (programAbsoluteFinish - programAbsoluteStart) + " ms.");

		
	} // end of main()
	
	
	static void analyzeTimestampsInResults(Gson gson, List<ViewRow> result) {

		int resultsLookedAt = 0;
		
		printCenteredBanner("About to analyze results");

		JsonParser jp = new JsonParser(); 
		
		// Deliberately unlikely values
		long minimumDiff = 1000000;
		long maximumDiff = 0;
		
		for (ViewRow vr : result) {
			resultsLookedAt++;

			String viewValueString = (String) vr.value().toString();

			// System.out.println("analyzeTimestampsInResults: " + viewValueString);

			com.google.gson.JsonObject valueJO = jp.parse(viewValueString).getAsJsonObject();
						
			long viewDateNow  = valueJO.get("viewDateNow").getAsLong();
			long creationDate = valueJO.get("creationDate").getAsLong();
			long diff = viewDateNow - creationDate;   // This difference should be positive
			System.out.println("Result #: " + resultsLookedAt 
					+ " creationDate: " + creationDate 
					+ " viewDateNow: "  + viewDateNow 
					+ " difference: "   + diff + " ms.");
			
			if (diff < minimumDiff) { minimumDiff = diff; }
			if (diff > maximumDiff) { maximumDiff = diff; }
			
			// Here is what this program is all about:
			// t2 is right now.  Compare creationDate from the doc and viewDateNow from the view
			// to right now, and report the time difference.
			
			long t2 = System.currentTimeMillis();
			
			logMessage("The item was first created " + ( t2 - creationDate) + " ms ago.");
			logMessage("The view emitted its value " + ( t2 - viewDateNow)  + " ms ago.");
			
		}

		logMessage("The minimum diff is: " + minimumDiff + "  ms.");
		logMessage("The maximum diff is: " + maximumDiff + "  ms.");
		printCenteredBanner("Done with analyze results ( looked at " + resultsLookedAt + ")");

		
	}
	
	
	static void runATimingClass(TimingClass tc) {
		tc.performTest();
		
		if (tc.didExceptionOccur()) {
			printCenteredBanner(tc.getClass().getName() + ": An exception did occur");
			tc.getException().printStackTrace();
			System.exit(1);
		}
		else {
			printCenteredBanner(tc.getClass().getName() + ": No exception occurred");
		}
			
		printCenteredBanner(tc.getClass().getName() + ": Elapsed time: " + tc.getElapsedTime() + " ms.");		
	}
	
	
	public static void printDecoration(int c, String s) {
		for (int i = 0; i < c; i++) { System.out.print(s); }
	}

	public static void printCenteredBanner(String s) {
		int numDecorations = ((SCREENCOLUMNS - (s.length() + 2)) / 2);
		printDecoration(numDecorations,"=");
		System.out.print(" " + s + " ");
		printDecoration(numDecorations,"=");		
		System.out.println();
	}
	
	static void logMessage(String s) {
		System.out.println("=== " + s + " ===");
	}
	
	
} // ViewTimingTest


class TimingClass {
	
	long      startTime, endTime;
	boolean   exceptionOccurred;
	Exception caughtException;
	
	public TimingClass() {
		startTime         = 0;
		endTime           = 0;
		caughtException   = null;
		exceptionOccurred = false;
	}
	
	public void startTiming() {		startTime = System.currentTimeMillis();    }
	public void stopTiming()  {		endTime   = System.currentTimeMillis();    }

	public long getElapsedTime() { 
		return (endTime - startTime);
	}
	
	public boolean didExceptionOccur() { return exceptionOccurred; }
	
	public Exception getException() { return caughtException; }
	
	// override in subclass
	public void doTheWork() throws Exception {
		ViewTimingTestRoundtrip.printCenteredBanner("This is where you do something");
	}
	
	public void performTest() {
		
		ViewTimingTestRoundtrip.printCenteredBanner(this.getClass().getName());

		// call the method that can be overridden
		startTiming();
		try {
			doTheWork();
		} catch (Exception e) {
			caughtException = e;
			exceptionOccurred = true;
		}
		stopTiming();
	}
	
} // generic TimingClass, each specific operation below is a subclass of it and implements the doTheWork() method

class CBSingleGetItemsTimer extends TimingClass {
	String[] ids;
	Bucket bucket;
	int successCount;
	
	public CBSingleGetItemsTimer(Bucket b, String[] idList) {
		bucket = b;
		ids = idList;
		successCount = 0;
	}
		
	public int getSuccessCount() { return successCount; }
	
	public void doTheWork() throws Exception {
		Document d;
		for (int i = 0; i < ids.length; i++) {
			d = bucket.get(ids[i]);
			if (d != null) { successCount++; }
		}
	}
} // Given a list of specific documents, get them sequentially


class CBConnectTimer extends TimingClass {

	CouchbaseCluster sourceCluster;
	String hostName;
	
	public CBConnectTimer(String s) {
		hostName = s;
	}
	
	public CouchbaseCluster getCluster() { return sourceCluster; };
	
	public void doTheWork() throws Exception {
		sourceCluster = CouchbaseCluster.create(hostName);
	}

} // CBConnectTimer



class CBOpenBucketTimer extends TimingClass {
	
	CouchbaseCluster myCluster;
	Bucket bucket;
	String bucketName;
	
	// Given a cluster and a bucket name, open the bucket
	public CBOpenBucketTimer(CouchbaseCluster c, String bName) {
		myCluster  = c;
		bucketName = bName;
	}
	
	public Bucket getBucket() { return bucket; };
	
	public void doTheWork() throws Exception {
		bucket = myCluster.openBucket(bucketName);	
	}
	
} // Open a bucket


class CBCreateBucketTimer extends TimingClass {

	// This is for bucket creation only.
	
	CouchbaseCluster myCluster;
	String bucketName;
	String u;
	String p;
	
	int bucketQuota = 100; // megabytes
	
	// Given a cluster and a bucket name, open the bucket
	public CBCreateBucketTimer(CouchbaseCluster c, String bName, String un, String pw) {
		myCluster  = c;
		bucketName = bName;
		u = un;
		p = pw;
	}
	
	public void doTheWork() throws Exception {
		DefaultBucketSettings.Builder bb = DefaultBucketSettings.builder();
		bb.name(bucketName);
		bb.quota(bucketQuota);
		BucketSettings bs = bb.build();
		
		ClusterManager cm = myCluster.clusterManager(u,p);
		cm.insertBucket(bs);
		
	}
	
} // Create a bucket


class CBFullViewQueryTimer extends TimingClass {
	
	Bucket bucket;
	String designDocName;
	String viewName;
	int totalResults;
	Stale staleValue;
	List<ViewRow> viewRowList;
	
	public CBFullViewQueryTimer(Bucket b, String dn, String vn, Stale stl) {
		bucket        = b;
		designDocName = dn;
		viewName      = vn;
		totalResults  = 0;
		staleValue = stl;
	}

	public int getTotalResults() { return totalResults; }
	
	public List<ViewRow> getViewResult() { return viewRowList; }
	
	public void doTheWork() throws Exception {

		// Perform the ViewQuery
		ViewResult viewResult = bucket.query(ViewQuery.from(designDocName, viewName).stale(staleValue));

		//boolean  success = result.success();
		//JsonObject debug = result.debug();
		//int    totalRows = result.totalRows();
				
		viewRowList = new ArrayList<ViewRow>();
		
		// Iterate through the returned ViewRows
		for (ViewRow row : viewResult) {
		    System.out.println("CBFullViewQueryTimer:" + row);
		    viewRowList.add(row);
		    totalResults++;
		}
	}
	
} // CBFullViewQueryTimer


class CBPopulateBucketTimer extends TimingClass {
	
	Bucket bucket;
	int numDocumentsToInsert;
	
	public CBPopulateBucketTimer(Bucket b, int numDocs) {
		bucket = b;
		numDocumentsToInsert = numDocs;
	}
	
	
	public void doTheWork() throws Exception {
	
		String DOCUMENTNAMEPREFIX = "testDocument";
		String jsonDocumentString = "";
		String documentKey        = "";
		JsonObject jsonObject     = null;
		
		long timeNow = System.currentTimeMillis();
		
		for (int i = 0; i < numDocumentsToInsert; i++) {
	
			// create a document
			documentKey = DOCUMENTNAMEPREFIX + i;
			jsonDocumentString = "{ \"name\" : \"testDocument\", \"serialNumber\" : " + i + ", \"creationDate\" : " + timeNow + " }";
			jsonObject = JsonObject.fromJson(jsonDocumentString);
			JsonDocument jsonDocument = JsonDocument.create(documentKey, jsonObject);

			// insert the document
			bucket.insert(jsonDocument);			
			
		} // for each document
		
	} // doTheWork
	
} // populate a bucket



class CBCreateDesignDocumentTimer extends TimingClass {
	
	Bucket bucket;
	String designDocumentName;
	String viewName;
	String mapFunction;
	
	public CBCreateDesignDocumentTimer(Bucket b, String ddn, String vn, String mf) {
		bucket             = b;
		designDocumentName = ddn;
		viewName           = vn;
		mapFunction        = mf;
	}
	
	public void doTheWork() throws Exception {
		
		View v = DefaultView.create(viewName, mapFunction);
		
		List<View> listOfViews = new ArrayList<View>();
		listOfViews.add(v);
		
		DesignDocument dd = DesignDocument.create(designDocumentName, listOfViews);
		
		BucketManager bm = bucket.bucketManager();
		bm.insertDesignDocument(dd);
	}
	
} // create a design document

// EOF