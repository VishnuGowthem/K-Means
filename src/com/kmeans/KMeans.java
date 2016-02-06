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
	
	// Delimiter to distinguish center points
	public static String delimiter = "\t| ";
	
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
		
		// Because for subsequent runs different folder names have to be specified.
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
			conf.setMapOutputKeyClass(Datapoint.class);
			conf.setMapOutputValueClass(Datapoint.class);
			
			//Reducer configurations.
			conf.setReducerClass(Reduce.class);
			
			// Input/Output configurations
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
			Path outflie_path = new Path(output + outputfile);
			
			// Write to outputfile after each iteration.
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(outflie_path)));
			List<Double> centers_next = new ArrayList<Double>();
			String line = br.readLine();
			while (line != null) {
				String[] sp = line.split("\t| ");
				double c = Double.parseDouble(sp[0]);
				centers_next.add(c);
				line = br.readLine();
			}
			br.close();

			//Checking convergence
			if (iter_count == 0) {
				previous = input_folder + centroidsfile;
			} else {
				previous = input + outputfile;
			}
			Path prevfile = new Path(previous);
			FileSystem fs1 = FileSystem.get(new Configuration());
			BufferedReader br1 = new BufferedReader(new InputStreamReader(
					fs1.open(prevfile)));
			List<Double> centers_prev = new ArrayList<Double>();
			String l = br1.readLine();
			while (l != null) {
				String[] sp1 = l.split(delimiter);
				double d = Double.parseDouble(sp1[0]);
				centers_prev.add(d);
				l = br1.readLine();
			}
			br1.close();

			// Convergence check done.
			Collections.sort(centers_next);
			Collections.sort(centers_prev);

			Iterator<Double> it = centers_prev.iterator();
			for (double d : centers_next) {
				double temp = it.next();
				if (Math.abs(temp - d) <= 0.1) {
					converged = true;
				} else {
					converged = false;
					break;
				}
			}
			++iter_count;
			
			// Setting up future iterations.
			input = output;
			output = output_folder + System.nanoTime();
		}
	}	
	

	/* Map Reduce Methods */
	/* We need to override the configure function in the mapper class.
	 * The idea is to have kCentroids Array list populated with centroids value.
	 * The values are read from the centroids file(for 1st iteration) and output file(for subsequent iterations)
	 * The input files are got from the Distributed cache files, set earlier in KMeans method.
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Datapoint, Datapoint> {

		/* Configure is basically modified from the initialize function in SimpleKMeans implementation */
		@Override
		public void configure(JobConf job) {
			try {
				String cvsdelimiter = ",";
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
						String[] temp = line.split(cvsdelimiter);

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
				System.err.println("Exception reading DistribtuedCache: " + e);
			}
		}

		/* After Configurations, the Map function now reads the points and
		 * will be able to find the nearest center to the point and emit it to
		 * the reducer. <Center, point> <Key, values> are emitted to the reducer.
		 */
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Datapoint, Datapoint> output,
				Reporter reporter) throws IOException {

	}
}
	
	public static class Reduce extends MapReduceBase implements
			Reducer<DoubleWritable, DoubleWritable, DoubleWritable, Text> {

		/*
		 * Reduce function will emit all the points to that center and calculate
		 * the next center for these points
		 */
		@Override
		public void reduce(DoubleWritable key, Iterator<DoubleWritable> values,
				OutputCollector<DoubleWritable, Text> output, Reporter reporter)
				throws IOException {

		}
	}
}