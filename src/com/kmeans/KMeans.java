package com.kmeans;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

class Datapoint implements WritableComparable<Datapoint> {
	double x;
	double y;
	
	public Datapoint() {
		super();
	}
	
	public Datapoint(double x, double y) {
		super();
		this.x = x;
		this.y = y;
	}
	
	public Datapoint(Datapoint point) {
		super();
		this.x = point.x;
		this.y = point.y;
		// TODO Auto-generated constructor stub
	}

	public double getX() {
		// TODO Auto-generated method stub
		return this.x;
	}
	
	public double getY() {
		// TODO Auto-generated method stub
		return this.y;
	}
	
	@Override
	public String toString() {
		return (this.x + "," + this.y);
	}

	public Datapoint get() {
		// TODO Auto-generated method stub
		return this;
	}
	
	 @Override
	 public void write(DataOutput out) throws IOException {
	  out.writeInt(2);
	  out.writeDouble(this.x);
	  out.writeDouble(this.y);
	 }
	 
	 @Override
	 public void readFields(DataInput in) throws IOException {
	  //int size = in.readInt();
	  this.x = in.readDouble();
	  this.y = in.readDouble();
	 }
	
	 @Override
	 public int compareTo(Datapoint a) {
	 
	  //boolean equals = true;
	  if ((this.x == a.x) && (this.y == a.y)){
		  return 0;
	  }
	  return 1;
	 }
}

public class KMeans {
	
	/* Global Configurations */
	// Default folders maybe specified here
	public static String output_folder = "";
	public static String input_folder = "";
	
	// Input data file and centroids file
	public static String datafile = "/data.txt";
	public static String centroidsfile = "/centroid.txt";
	
	// Named in standard part-nnnn format for mapreduce configurations
	public static String outputfile = "/part-00000";
	
	// Delimiter to distinguish center and other points
	public static String delimiter = "\t| ";
	
	// Delimiter to distinguish x and y of x,y points
	public static String cvsdelimiter = ",";
	
	// List of datapoints to store centroids
	public static List<Datapoint> kCentroids = new ArrayList<Datapoint>();
	
	/* Main Class */
	public static void main(String[] args) throws Exception {
		kMeans(args);
	}

	/* KMeans Method */
	public static void kMeans(String[] args) throws Exception {
		input_folder = args[0];
		output_folder = args[1];
		int iter_count = 0;
		boolean converged = false;
		String previous = "";
		
		// Because for subsequent Iterations different folder names have to be specified.
		String output = output_folder + System.nanoTime();
		
		// Input for iterations after the first.
		String input = output;

		// Iterating Till Convergence.
		while (converged == false) {
			JobConf conf = new JobConf(KMeans.class);
			if (iter_count == 0) {
				// So we use the centroids file for first iteration and output file of previous iterations for subsequent iterations.
				Path hdfsPath = new Path(input_folder + centroidsfile);
				// Uploading file to HDFS distributed system.
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			} else {
				Path hdfsPath = new Path(input + outputfile);
				// Uploading file to HDFS distributed system.
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			}

			// Jobconf configurations.s
			conf.setJobName("KMeans");
			
			// Mapper configurations
			conf.setMapperClass(Map.class);
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			
			//Reducer configurations.
			conf.setReducerClass(Reduce.class);
			
			// Input/Output configurations
			//conf.setOutputKeyComparatorClass(Datapoint.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputKeyClass(Datapoint.class);
			conf.setOutputValueClass(Text.class);
			conf.setOutputFormat(TextOutputFormat.class);

			// Setting Input file (Data file path)
			FileInputFormat.setInputPaths(conf,
					new Path(input_folder + datafile));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			//Command to run Job
			JobClient.runJob(conf);

			// Outputfile path
			Path outfile_path = new Path(output + outputfile);
			
			List<Datapoint> centers_next = construct_Datapointlist(outfile_path);
			
			/*
			// Read Outputfile and get centroids value
			FileSystem outfs = FileSystem.get(new Configuration());
			BufferedReader outreader = new BufferedReader(new InputStreamReader(outfs.open(outfile_path)));
			List<Datapoint> centers_next = new ArrayList<Datapoint>();
			String line = outreader.readLine();
			while (line != null) {
				String[] temp2 = line.split(delimiter);
				String[] temp = temp2[1].split(",");
				Datapoint current = new Datapoint(Double.parseDouble(temp[0]),Double.parseDouble(temp[1]));
				centers_next.add(current);
				line = outreader.readLine();
			}
			outreader.close();
			*/
			
			//Checking convergence
			if (iter_count == 0) {
				previous = input_folder + centroidsfile;
			} else {
				previous = input + outputfile;
			}
			Path prevfile_path = new Path(previous);
			List<Datapoint> centers_current = construct_Datapointlist(prevfile_path);
			int k = centers_current.size();
			
			/*
			FileSystem prevfs = FileSystem.get(new Configuration());
			BufferedReader curreader = new BufferedReader(new InputStreamReader(prevfs.open(prevfile)));
			List<Datapoint> centers_prev = new ArrayList<Datapoint>();
			String line2 = curreader.readLine();
			while (line2 != null) {
				String[] temp2 = line.split(delimiter);
				String[] temp = temp2[1].split(",");
				Datapoint current = new Datapoint(Double.parseDouble(temp[0]),Double.parseDouble(temp[1]));
				centers_prev.add(current);
				line2 = curreader.readLine();
			}
			curreader.close();
			*/

			/*
			// Convergence check done.
			//Collections.sort(centers_next);
			//Collections.sort(centers_prev);

			
			//converged = checkconvergence(iter_count, k_value);
			Iterator<Double> it = centers_current.iterator();
			for (double d : centers_next) {
				double temp = it.next();
				if (Math.abs(temp - d) <= 0.1) {
					converged = true;
				} else {
					converged = false;
					break;
				}
			}
			*/
			
			// Convergence check
			converged = checkconvergence(centers_current, centers_next, iter_count, k);
			++iter_count;
			
			// Setting up future iterations.
			input = output;
			output = output_folder + System.nanoTime();
		}
		
	}	
	
	public static List<Datapoint> construct_Datapointlist(Path filepath) throws Exception{  
		FileSystem filesystem = FileSystem.get(new Configuration());
		BufferedReader reader = new BufferedReader(new InputStreamReader(filesystem.open(filepath)));
		List<Datapoint> center = new ArrayList<Datapoint>();
		String line = reader.readLine();
		
		while (line != null) {
			System.out.println("Reading line " + line);
			String[] temp2 = line.split(delimiter);
			//System.out.println(temp2[0]);
			//System.out.println(temp2[2]);
			String[] temp = temp2[0].split(cvsdelimiter);
			System.out.println(temp[0]);
			System.out.println(temp[1]);
			Datapoint current = new Datapoint(Double.parseDouble(temp[0]),Double.parseDouble(temp[1]));
			center.add(current);
			line = reader.readLine();
		}
		reader.close();
		return center;
	}

	
	public static boolean checkconvergence(List<Datapoint> centers_current, List<Datapoint> centers_next, int iter_count, int k) throws Exception{
		//Convergence done by iteration count 
		if (iter_count == 10){
			return true;
		}
		else {
			// Convergence by checking previous and current centers
			int index = 0;
			double edist = 0.0;
			while (index != k){
				//System.out.println("Index is " + index + " k value is " + k);
				if ((centers_current.isEmpty() == false) && (centers_next.isEmpty() == false)){
					edist = Euclideandistance(centers_current.get(index),centers_next.get(index));
					if (edist != 0.0){
						return false;
						}
					}
				index++;
				}
			}
		return true;
		}

	public static double Euclideandistance(Datapoint a, Datapoint b) throws IOException{
		// Euclidean distance between two points a(x1,y1) and b(x2,y2) is d(a,b) = squareroot[(x2-x1)^2 + (y2-y1)^2]
		double edist = 0.0;
		edist = Math.sqrt(Math.pow((b.x-a.x), 2) + Math.pow((b.y-a.y),2));
		System.out.println("Euclidean distance between " + a + " and " + b + " is " + edist);
		return Math.abs(edist); 
	}
	
	// For Debugging as cant print values in MapReduce class
	public static void printValues(Text values){
		System.out.println("**** printValues *****");
		System.out.println(values);
	}
	
	public static void printStringarray(String[] sample){
		System.out.println("**** printStringarray *****");
		System.out.println(sample[0]);
		//System.out.println(sample[1]);
	}
	
	public static void printString(String sample){
		System.out.println("**** printString *****");
		System.out.println(sample);
		//System.out.println(sample[1]);
	}
	/* Map Reduce Methods */
	/* We need to override the configure function in the mapper class.
	 * The idea is to have kCentroids Array list populated with centroids value.
	 * The values are read from the centroids file(for 1st iteration) and output file(for subsequent iterations)
	 * The input files are got from the Distributed cache files, set earlier in KMeans method.
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		/* Configure is basically modified from the initialize function in SimpleKMeans implementation */
		@Override
		public void configure(JobConf job) {
			try {
				
				// Read from distributed cache files.
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
				if (cacheFiles != null && cacheFiles.length > 0) {
					String line = "";
					BufferedReader in = null;
					kCentroids.clear();
					try {
						in = new BufferedReader(new FileReader(cacheFiles[0].toString()));
						while ((line = in.readLine()) != null) {

					    // use comma as separator as there are x,y values
						String[] temp2 = line.split(delimiter);
						//printString(temp2);
						String[] temp = temp2[0].split(cvsdelimiter);
						//printString(temp);

						Datapoint temppoint = new Datapoint(Double.parseDouble(temp[0]),Double.parseDouble(temp[1]));
						kCentroids.add(temppoint);
						//System.out.println(kCentrois);
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
				}
			} catch (IOException e) {
				System.err.println("Exception reading DistributedCache: " + e);
			}
		}

		/* After Configurations, the Map function now reads the points and
		 * will be able to find the nearest center to the point and emit it to
		 * the reducer. <Center, point> <Key, values> are emitted to the reducer.
		 */
		 @Override
		 public void map(LongWritable key, Text value,
		     //OutputCollector<Datapoint, Datapoint> output,
				 OutputCollector<Text, Text> output,
		     Reporter reporter) throws IOException {
		   String line = value.toString();
		   String cvsdelimiter = ",";
			String[] temp = line.split(cvsdelimiter);
			//String[] temp = temp2[1].split(cvsdelimiter);
			//printString(temp);
			Datapoint point = new Datapoint(Double.parseDouble(temp[0]),Double.parseDouble(temp[1]));
		   double mindist = Double.MAX_VALUE;
		   double newdist = Double.MAX_VALUE;
		   Datapoint nearest_center = kCentroids.get(0);
		   // Finding the minimum center for a point
			for (Datapoint center: kCentroids){
				newdist = Euclideandistance(point, center);
				//System.out.println("prevnearestcenter : "+nearestcenter);
				//System.out.println("mindist : " + mindist);
				//System.out.println("newdist : " + newdist);
				if (Math.abs(newdist) < Math.abs(mindist)){
					mindist = newdist;
					nearest_center = center;
					//centerindex = kCentroids.indexOf(center);
				}
			}
		   
		   // Emit the nearest center and the point
		   //output.collect(new Datapoint(nearest_center), new Datapoint(point));
		   output.collect(new Text(nearest_center.toString()), new Text(point.toString()));
		 }
		}

	
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Datapoint, Text> {

		/*
		 * Reduce function will emit all the points to that center and calculate
		 * the next center for these points
		 */
		 @Override
		 public void reduce(Text key, Iterator<Text> values,
		     OutputCollector<Datapoint, Text> output, Reporter reporter)
		     throws IOException {
		   Datapoint newCenter = new Datapoint(0.0,0.0);
		   Datapoint sum = new Datapoint(0.0,0.0);
		   int count = 0;
		   String points = "";
		   while (values.hasNext()) {
			 //printValues(values.next());
			 String line = values.next().toString();  
		    String[] temp = line.split(cvsdelimiter);
				//String[] temp = temp2[1].split(cvsdelimiter);
			Datapoint d = new Datapoint(Double.parseDouble(temp[0]),Double.parseDouble(temp[1])); 
		     points = points + " " + d.toString();
		     sum.x = sum.x + d.x;
		     sum.y = sum.y + d.y;
		     ++count;
		   }

		   // Calculation of new Center
		   newCenter.x = sum.x / count;
		   newCenter.y = sum.y / count;
		   String ncenter = newCenter.toString();
		   printString(ncenter);

		   // Emit the new center and point
		   output.collect(new Datapoint(newCenter), new Text(points));
		 }
	}
}