# K-Means
Implementation of K Means in Map Reduce

Workflow :
==========
* Simple K Means inmplementation
* Hadoop Installation
* KMeans Implementation in MapReduce
* Testing and Validation

(Check Workflow pdf)

Simple K-Means Algorithm :
===========================

1. Choose the smallest possible K value possible.
	1a. Choose random k data points as widely spaced as possible as initial centers from the Input data set.
	1b. Calculate distance from each center for each of the data point. The distance measure here is Euclidean measure as the input is (x,y) points.
	1c. Group or Cluster data points to center with least distance.
	1d. Compute New center from all the data points by cluster. (Mean value of all points in that cluster)
	1e. Repeat steps 1b, 1c, 1d
	1f. Stop iterations when there is no/negligible change in center or when maximum iterations count is reached.
2. Repeat steps 1a to 1f for next K value till maximum possible k value.
3. Choose k value and corresponding clustering based on the "Elbow theorem" after plotting SSE of all calculated K values.

K-Means Algorithm in Map Reduce :
==================================
Extending the same algorithm in Map Reduce, the following steps need to be done.

1. Map Reduce Job configurations are to be made.
	- Add Distributed file paths.
	- Set Output class, Map Output Class, for Key and Value to be emitted from Mapper to Reducer.
	- Set Inputformat, Outputformat (Using textformat currently but can be done with csv files as well)
	
2. Mapper Functionalities implemented.

Mapper :
Mapper Reads centroid files from distributed system.
will send (center, point) outputs after clustering to the reducer.
Also for the initial iteration centroids are to be given whereas in future iterations Mapper reads from the same file which will be populated with new centers.

Reducer :
Reducer collects the (center,point) from the mapper and sends it to the centers.
Calculates the new center for subsequent iteration and writes it to the output file, which are used as centroids file in future iterations.

Dependent jars needed for Kmeans-MapReduce :
=============================================
1.hadoop-core-1.2.1.jar
2.org.apache.commons.httpclient.jar
3.apache-commons-lang.jar
4.com.springsource.org.codehaus.jackson-1.4.2.jar
5.com.springsource.org.codehaus.jackson.mapper-1.4.2.jar
6.commons-configuration-1.7.jar
7.commons-io-2.4.jar
8.commons-logging-1.1.1.jar

Some of the above jars might not be needed as I was trying different things during the course of the project.

Run time Arguments :
===================
Specify the input_folder as args[0] and output_folder as args[1] as run time arguments 


Future Work :
==============

* Script to run for different k values and gather output.
* Python Script to visualize output
* Automate/Integrate all works 
