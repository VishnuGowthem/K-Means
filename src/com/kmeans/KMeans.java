package com.kmeans;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

@SuppressWarnings("deprecation")
public class KMeans {
	public static Map<Datapoint, List<Datapoint>> output = new HashMap<Datapoint, List<Datapoint>>();
	public static List<Datapoint> data = new ArrayList<Datapoint>();
	public static List<Datapoint> kCentroids = new ArrayList<Datapoint>();
	public static List<Datapoint> newkCentroids = new ArrayList<Datapoint>();
	public static String datafile = "/data.csv";
	public static String centroidsfile = "/centroid";
	public static String outputfile = "/output";
	
	public void main(String[] args)throws Exception {
		kmeans(args);
	}
	
	public void kmeans(String[] args) throws Exception {
		String input_folder = args[0]; //DataSet File Path
		int k_value = Integer.parseInt(args[1]); //K-Value for current execution
		
		int iter_count = 0;
		boolean converged = false;
		while(true){
			JobConf conf = new JobConf(KMeans.class);
			if (iter_count == 0)
			{
				// Instead of giving centers as input, have taken them from the data file.
				initialize(input_folder, k_value);
				// Centroids file uploaded to hdfs. Will Overwrite any existing copy.
				Path hdfspath = new Path(input_folder + centroidsfile);
				DistributedCache.addCacheFile(hdfspath.toUri(), conf);
				//System.out.println("Calling clustering " + iter_count);
				//clustering(input_folder, iter_count);
			}
			else
			{
				Path hdfspath = new Path(input_folder + outputfile);
				DistributedCache.addCacheFile(hdfspath.toUri(), conf);
				//System.out.println("kCentroids are " + kCentroids);
				//kCentroids = newkCentroids;
				//kCentroids.clear();
				//int index = 0;
				//while(index < newkCentroids.size()){
					//kCentroids.add(index, newkCentroids.get(index));
					//index++;
				//}
				//System.out.println("KCentroids before clustering are " + kCentroids);
				//System.out.println("Calling clustering " + iter_count);
				//clustering(input_folder, iter_count);	
			}
			
			conf.setJobName("KMeans");
			
			//System.out.println("Computing new Centroids");
			//computenewcentroids(input_folder, k_value, iter_count, false);
			//System.out.println("kCentroids are " + kCentroids);
			//System.out.println("newKCentroids are " + newkCentroids);
			//System.out.println("Calling checkconvergence");
			/*
			converged = checkconvergence(iter_count, k_value);
			if (converged != true)
			{
				iter_count++;
				//output.clear();	
			}
			else
			{
				System.out.println("Done Clustering. \nKindly check files numbered output0,output1.. for results in " + input_folder);
				break;
			}
			*/
		}
	}
	
	// Method stores all data points to data ArrayList. 
	// Also Random data points are chosen as centroids to kCentroids ArrayList for the first iteration.
	public void initialize(String input_folder, int k) throws Exception{
		BufferedReader in = null;
		String line = "";
		String cvsdelimiter = ",";
		String csvfile = input_folder + datafile;
		
		try {
			in = new BufferedReader(new FileReader(csvfile));
			while ((line = in.readLine()) != null) {

		        // use comma as separator
			String[] temp = line.split(cvsdelimiter);

			Datapoint temppoint = new Datapoint(Double.parseDouble(temp[0]),Double.parseDouble(temp[1]));
			data.add(temppoint);
			//System.out.println(data);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		computenewcentroids(input_folder, k, 0, true);
	}
	
	// Method to compute centroids during several iterations
	public void computenewcentroids(String input_folder, int k, int iter_count, boolean random) throws Exception{
		int length = data.size();
		int centroidcount = 0;
		int temp = 0;
		String centroidfile = input_folder + centroidsfile + iter_count;
		PrintWriter writer = new PrintWriter(centroidfile, "UTF-8");
		if (random == true){
			kCentroids.clear();
			// Centroids are computed for the first time. 
			// Its enough if widely spaced random data points are picked as Centroids.
			while (centroidcount < k) {
				kCentroids.add(data.get(temp));
				writer.println(centroidcount + "|" + data.get(temp));
				temp = temp + length/k;
				centroidcount++;
				}
		}
		else {
			newkCentroids.clear();
			// Centroids are calculated as mean of all data points.
			for (Datapoint center: kCentroids){
				//System.out.println("center value is " + center);
				int valuescount = 0;
				Datapoint sum = new Datapoint(0.0,0.0);
				for (Datapoint point:output.get(center)){
					sum.x = sum.x + point.x;
					sum.y = sum.y + point.y;
					valuescount++;
				}
				//System.out.println("Sum x is " + sum.x + " Sum y is " + sum.y);
				//System.out.println("Values count is " + valuescount);
				Datapoint newcentroid = new Datapoint(sum.x/valuescount, sum.y/valuescount);
				newkCentroids.add(newcentroid);
				writer.println(centroidcount + "|" + newcentroid);
				centroidcount++;
			}
		}
		writer.close();
		//System.out.println("kCentroids are " + kCentroids);
		//System.out.println("newKCentroids are " + newkCentroids);
	}
}