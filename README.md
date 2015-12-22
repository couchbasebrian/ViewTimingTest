# ViewTimingTest
Basic framework for experimenting with multi-get vs view query

This was created to time various operations with respect to Views.  When this program is run it connects to the cluster and creates a bucket.  It populates the bucket with test data, and creates a design document and view on that bucket.  After the program commpletes, the bucket is not deleted by the program, so that you can view the data if desired.

The output looks something like this

    === Time to connect:                195 ms
    === Time to create bucket:          7286 ms
    === Time to open bucket:            185 ms
    === Time to create design document: 49 ms
    === Time to populate bucket:        2538 ms
    === Time to get single items:       41 ms
    === Time to do full view query (with stale = false):         2530 ms
    === Time to do full view query (with stale = update_after):  23 ms

# ViewTimingTestRoundtrip
New version that places timestamps into the document and measures them after retrieval

    creationDate: 1450744273869 viewDateNow: 1450744277857 diff: 3988 ms.
    === The item was first created 64855 ms ago. ===
    === The view emitted its value 60867 ms ago. ===
    === The minimum diff is: 3021  ms. ===
    === The maximum diff is: 4271  ms. ===
    =========================== Done with analyze results ( looked at 1000) ===========================
    === Got the expected results.  Done with polling test. ===
    === Time to connect:                                         189 ms. ===
    === Time to create bucket:                                   5191 ms. ===
    === Time to open bucket:                                     183 ms. ===
    === Time to create design document:                          41 ms. ===
    === Time to populate bucket:                                 2141 ms. ===
    === Time to get single items:                                28 ms. ===
    === Time to do full view query (with stale = false):         2436 ms. ===
    === Time to do full view query (with stale = update_after):  200 ms. ===
    
