// June 11, 2015
// Brian Williams
// ViewTimingTest
// Developed with Couchbase Java SDK version 2.1.3 available from:
// http://packages.couchbase.com/clients/java/2.1.3/Couchbase-Java-Client-2.1.3.zip
//
// Given a cluster, connect
// Create a design document
// Create a view
// Create a bucket
// Insert data into the bucket
// Query the view for some data
// Query the bucket for some of the items
// Compare getting items individually vs. getting same items from a specific view

package com.couchbase.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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


public class ViewTimingTest {

	static final int SCREENCOLUMNS = 100;	// adjust to fit your terminal

	public static void main(String[] args) {

		String HOSTNAME           = "192.168.0.1";  
		String USERNAME           = "Administrator";
		String PASSWORD           = "password";
		String BUCKETNAMEPREFIX   = "testBucket";
		int    MAXBUCKETNUMBER    = 1000;
		
		String DESIGNDOCUMENTNAME = "dd1";
		String VIEWNAME           = "vn1";
		
		String MAPFUNCTION        = "function (doc, meta) { if ((doc.serialNumber % 100) == 0) { emit(meta.id, doc); } } ";
		
		int    NUMDOCUMENTS       = 1000;
		
		// Connect to the cluster
		CBConnectTimer ct = new CBConnectTimer(HOSTNAME);
		runATimingClass(ct);
		CouchbaseCluster cluster = ct.getCluster();
		long timeToConnect = ct.getElapsedTime();
		
		// Come up with a bucket name and create a bucket
		int randomIdentifier = (int) (Math.random() * MAXBUCKETNUMBER);
		String newBucketName = BUCKETNAMEPREFIX + randomIdentifier;
		printCenteredBanner("Bucket name is " + newBucketName);
		CBCreateBucketTimer cbt = new CBCreateBucketTimer(cluster, newBucketName, USERNAME, PASSWORD);
		runATimingClass(cbt);
		long timeToCreateBucket = cbt.getElapsedTime();

		// Open the new bucket
		CBOpenBucketTimer bt = new CBOpenBucketTimer(cluster, newBucketName);
		runATimingClass(bt);
		Bucket bucket = bt.getBucket();
		long timeToOpenBucket = bt.getElapsedTime();
		
		// Create a prod design document and view on the bucket
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
		int expectedResults        = 10;
				
		while (gotExpectedResults == false) {
			fvqt1 = new CBFullViewQueryTimer(bucket, DESIGNDOCUMENTNAME, VIEWNAME, Stale.UPDATE_AFTER);
			runATimingClass(fvqt1);
			resultsSeen = fvqt1.getTotalResults();
			if (resultsSeen == expectedResults) {
				gotExpectedResults = true;
				timeToGetCompleteResultSet = fvqt1.getElapsedTime();
			} 
			logMessage("Iteration " + iterationCount + ": The total results count was " + fvqt1.getTotalResults());
			iterationCount++;
		}
		}
		
		// Print Results
		logMessage("Time to connect:                " + timeToConnect);
		logMessage("Time to create bucket:          " + timeToCreateBucket);
		logMessage("Time to open bucket:            " + timeToOpenBucket);
		logMessage("Time to create design document: " + timeToCreateDesignDocument);
		logMessage("Time to populate bucket:        " + timeToPopulateBucket);
		logMessage("Time to get single items:       " + timeToSingleGetItems);
		logMessage("Time to do full view query (with stale = false):         " + timeToFullViewQuery);
		logMessage("Time to do full view query (with stale = update_after):  " + timeToGetCompleteResultSet);

		// Clean up		
		bucket.close();
		cluster.disconnect();
		
		
	} // end of main()
	
	static void runATimingClass(TimingClass tc) {
		tc.performTest();
		
		if (tc.didExceptionOccur()) {
			printCenteredBanner("An exception did occur");
			tc.getException().printStackTrace();
			System.exit(1);
		}
		else {
			printCenteredBanner("An exception did not occur");
		}
	
		printCenteredBanner("Elapsed time: " + tc.getElapsedTime() + " ms.");		
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
		ViewTimingTest.printCenteredBanner("This is where you do something");
	}
	
	public void performTest() {
		
		ViewTimingTest.printCenteredBanner(this.getClass().getName());

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
	
} // TimingClass

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
}


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
	
}


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
	
}


class CBFullViewQueryTimer extends TimingClass {
	
	Bucket bucket;
	String designDocName;
	String viewName;
	int totalResults;
	Stale staleValue;
	
	public CBFullViewQueryTimer(Bucket b, String dn, String vn, Stale stl) {
		bucket        = b;
		designDocName = dn;
		viewName      = vn;
		totalResults  = 0;
		staleValue = stl;
	}

	public int getTotalResults() { return totalResults; }
	
	public void doTheWork() throws Exception {

		// Perform the ViewQuery
		ViewResult result = bucket.query(ViewQuery.from(designDocName, viewName).stale(staleValue));

		//boolean  success = result.success();
		//JsonObject debug = result.debug();
		//int    totalRows = result.totalRows();
				
		// Iterate through the returned ViewRows
		for (ViewRow row : result) {
		    System.out.println(row);
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
		
		for (int i = 0; i < numDocumentsToInsert; i++) {
	
			// create a document
			documentKey = DOCUMENTNAMEPREFIX + i;
			jsonDocumentString = "{ \"name\" : \"testDocument\", \"serialNumber\" : " + i + " }";
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
	
}


// EOF
