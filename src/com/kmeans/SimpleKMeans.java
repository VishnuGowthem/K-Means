package com.kmeans;

import java.io.IOException;
import java.util.*;
import java.io.*;



public class SimpleKMeans {
	
	public static Map<Datapoint, List<Datapoint>> output = new HashMap<Datapoint, List<Datapoint>>();
	public static List<Datapoint> data = new ArrayList<Datapoint>();
	public static List<Datapoint> kCentroids = new ArrayList<Datapoint>();
	public static List<Datapoint> newkCentroids = new ArrayList<Datapoint>();
	public static String datafile = "/data.csv";
	public static String centroidsfile = "/centroid";
	public static String outputfile = "/output";
	
	public static void main(String[] args)throws Exception {
		simpKmeans(args);
	}
	
	public static void simpKmeans(String[] args) throws Exception {
		String input_folder = args[0]; //DataSet File Path
		int k_value = Integer.parseInt(args[1]); //K-Value for current execution
		
		int iter_count = 0;
		boolean converged = false;
		while(true){
			if (iter_count == 0)
			{
				//System.out.println("Calling initialize");
				initialize(input_folder, k_value);
				//System.out.println("Calling clustering " + iter_count);
				clustering(input_folder, iter_count);
			}
			else
			{
				//System.out.println("kCentroids are " + kCentroids);
				//kCentroids = newkCentroids;
				kCentroids.clear();
				int index = 0;
				while(index < newkCentroids.size()){
					kCentroids.add(index, newkCentroids.get(index));
					index++;
				}
				//System.out.println("KCentroids before clustering are " + kCentroids);
				//System.out.println("Calling clustering " + iter_count);
				clustering(input_folder, iter_count);	
			}
			//System.out.println("Computing new Centroids");
			computenewcentroids(input_folder, k_value, iter_count, false);
			//System.out.println("kCentroids are " + kCentroids);
			//System.out.println("newKCentroids are " + newkCentroids);
			//System.out.println("Calling checkconvergence");
			converged = checkconvergence(iter_count, k_value);
			if (converged != true)
			{
				iter_count++;
				output.clear();	
			}
			else
			{
				System.out.println("Done Clustering. \nKindly check files numbered output0,output1.. for results in " + input_folder);
				break;
			}
		}
	}
	
	// Method stores all data points to data ArrayList. 
	// Also Random data points are chosen as centroids to kCentroids ArrayList for the first iteration.
	public static void initialize(String input_folder, int k) throws Exception{
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
	public static void computenewcentroids(String input_folder, int k, int iter_count, boolean random) throws Exception{
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
				//writer.println(centroidcount + "|" + newcentroid);
				writer.println(newcentroid);
				centroidcount++;
			}
		}
		writer.close();
		//System.out.println("kCentroids are " + kCentroids);
		//System.out.println("newKCentroids are " + newkCentroids);
	}
	
	public static void clustering(String input_folder, int iter_count) throws Exception{
		String outputfilename = input_folder + outputfile + iter_count;
		PrintWriter writer = new PrintWriter(outputfilename, "UTF-8");
		// Finding the minimum center for a point
		for (Datapoint point: data) {
			int centerindex = 0;
			double mindist = Double.MAX_VALUE;
			double newdist = Double.MAX_VALUE;
			Datapoint nearestcenter = kCentroids.get(0);
			for (Datapoint center: kCentroids){
				newdist = Euclideandistance(point, center);
				//System.out.println("prevnearestcenter : "+nearestcenter);
				//System.out.println("mindist : " + mindist);
				//System.out.println("newdist : " + newdist);
				if (Math.abs(newdist) < Math.abs(mindist)){
					mindist = newdist;
					nearestcenter = center;
					centerindex = kCentroids.indexOf(center);
				}
			}
			writer.println(centerindex + "|" + nearestcenter + "|" + point);
			addtoOutput(nearestcenter,point);
		}
		writer.close();
	}
	
	public static void addtoOutput(Datapoint center, Datapoint point) throws Exception{
		List<Datapoint> values = output.get(center);
		if (values == null){
			values = new ArrayList<Datapoint>();
		}
		values.add(point);
		output.put(center,values);
	}
	
	public static double Euclideandistance(Datapoint a, Datapoint b) throws Exception{
		// Euclidean distance between two points a(x1,y1) and b(x2,y2) is d(a,b) = squareroot[(x2-x1)^2 + (y2-y1)^2]
		double edist = 0.0;
		edist = Math.sqrt(Math.pow((b.x-a.x), 2) + Math.pow((b.y-a.y),2));
		//System.out.println("Euclidean distance between " + a + " and " + b + " is " + edist);
		return edist; 
	}
	
	public static boolean checkconvergence(int iter_count, int k) throws Exception{
		//Convergence currently done by iteration count 
		// Convergence to be added when previous and current centers are same.
		if (iter_count == 0){
			return true;
		}
		else {
			int index = 0;
			double edist = 0.0;
			while (index != k){
				//System.out.println("Index is " + index + " k value is " + k);
				if (newkCentroids.isEmpty() == false){
					edist = Euclideandistance(kCentroids.get(index),newkCentroids.get(index));
					if (edist != 0.0){
						return false;
						}
					}
				index++;
				}
			}
		return true;
		}

}
