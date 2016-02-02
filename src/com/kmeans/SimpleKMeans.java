package com.kmeans;

import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Reducer;
import java.awt.Point;

class Datapoint{
	final double x;
	final double y;
	
	public Datapoint(final double x, final double y) {
		this.x = x;
		this.y = y;
	}
	
	@Override
	public String toString() {
		return "" + x + "," + y;
	}
}

public class SimpleKMeans {
	
	public static List<Datapoint> data = new ArrayList<Datapoint>();
	public static List<Datapoint> kCentroids = new ArrayList<Datapoint>();
	public static String DATAPATH = "/data.csv";
	public static String CENTROID = "/centroid";
	public static String OUTPUT = "/output";
	
	public static void main(String[] args)throws Exception {
		simpKmeans(args);
	}
	
	public static void simpKmeans(String[] args) throws Exception {
		String input_folder = args[0]; //DataSet File Path
		int k_value = Integer.parseInt(args[1]); //K-Value for current execution
		
		int iter_count = 0;
		boolean converged = false;
		while(converged != true){
			if (iter_count == 0)
			{
				initialize(input_folder, k_value);
			}
			clustering(input_folder);
			iter_count++;
			//computecentroids(input_folder, k_value, false);
			converged = checkconvergence(iter_count);
		}
	}
	
	// Method stores all data points to data ArrayList. 
	// Also Random data points are chosen as centroids to kCentroids ArrayList for the first iteration.
	public static void initialize(String input_folder, int k) throws Exception{
		BufferedReader in = null;
		String line = "";
		String cvsdelimiter = ",";
		String csvfile = input_folder + DATAPATH;
		
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
		computecentroids(input_folder, k, true);
	}
	
	// Method to compute centroids during several iterations
	public static void computecentroids(String input_folder, int k, boolean random) throws Exception{
		int length = data.size();
		System.out.println("Length is " + length);
		int centroidcount = 0;
		int temp = 0;
		String centroidfile = input_folder + CENTROID;
		PrintWriter writer = new PrintWriter(centroidfile, "UTF-8");
		if (random == true){	
			// Centroids are computed for the first time. 
			// Its enough if widely spaced random data points are picked as Centroids.
			kCentroids.clear();
			while (centroidcount < k) {
				kCentroids.add(data.get(temp));
				writer.println(centroidcount + "|" + data.get(temp));
				temp = temp + length/k;
				centroidcount++;
				}
		}
		else {
			
		}
		writer.close();
	}
	
	public static void clustering(String input_folder) throws Exception{
		String outputfile = input_folder + OUTPUT;
		PrintWriter writer = new PrintWriter(outputfile, "UTF-8");
		// Finding the minimum center for a point
		for (Datapoint point: data) {
			double mindist = Double.MAX_VALUE;
			double newdist = Double.MAX_VALUE;
			Datapoint nearestcenter = kCentroids.get(0);
			int centerindex = 0;
			for (Datapoint center: kCentroids){
				newdist = Euclideandistance(point, center);
				if (Math.abs(newdist) < Math.abs(mindist)){
					mindist = newdist;
					nearestcenter = center;
					centerindex = kCentroids.indexOf(center);
				}
			}
			writer.println(centerindex + "|" + nearestcenter + "|" + point);
		}
		writer.close();
	}
	
	public static double Euclideandistance(Datapoint a, Datapoint b) throws Exception{
		// Euclidean distance between two points a(x1,y1) and b(x2,y2) is d(a,b) = squareroot[(x2-x1)^2 + (y2-y1)^2]
		double edist = 0.0;
		edist = Math.sqrt(Math.pow((b.x-a.x), 2) + Math.pow((b.y-a.y),2));
		System.out.println("Euclidean distance between " + a + " and " + b + " is " + edist);
		return edist; 
	}
	
	public static boolean checkconvergence(int iter_count) throws Exception{
		return true;
	}
}
