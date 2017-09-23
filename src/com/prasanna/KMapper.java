/*
 * This Kmeans mapper reads the cluster centers from the distributed cache and  
 * then for every row of data predicts in what cluster the data point belongs
 */

package com.prasanna;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KMapper extends Mapper<LongWritable,Text,IntWritable,Text> {
	private static final Log LOG = LogFactory.getLog(KMapper.class);
	
	List<Double> centers = new ArrayList<Double>(); // List to store the initial centroids
	int num_centers = 0;
	int dimensions = 0;
	
	
	// From the file in the distributed cache read and set the cluster centers
	public void setup(Context context) throws IOException{
		Path[] filepath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		BufferedReader br;
		String records; //variable to read every record in the cache
		String keyval[];
		String parsed[]; // variable to parse every record
		int i=0;
		int index;
		
		
		br = new BufferedReader(new FileReader(filepath[0].toString()));
		while((records=br.readLine())!=null){
			keyval = records.split("\t");
			parsed=keyval[1].split(",");
			dimensions=parsed.length;
			for (int j=0;j<dimensions;j++){
				centers.add(i,(double)0); //just adding 0 to populate the arraylist
				i++;
			}
			num_centers++;
		}
		br.close();
		
		// Reading and setting the centers form the file
		br = new BufferedReader(new FileReader(filepath[0].toString()));
		while ((records=br.readLine())!=null){ 
			keyval = records.split("\t");
			index = Integer.parseInt(keyval[0].toString());
			parsed = keyval[1].split(",");
			
			i=0;
			for (String str: parsed){
				centers.set((((index-1)*dimensions)+i) ,Double.parseDouble(str));
					i++;
			}
		}
		br.close();
		for(int m=0;m<centers.size();m++){
			LOG.info("Centers: " + centers.get(m));
		}
		LOG.info("Dimensions: "+dimensions);
	}
	
	// For every input point find the nearest center and output the point as a key-val pair
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		
		String val = values.toString();
		String elements[] = val.split(",");
		int index=0;
		int cluster_number=0;
		double[] dist = new double[num_centers];
		double min = Double.MAX_VALUE;
		
		//calculating the nearest cluster to the point
		for(int k=0;k<num_centers;k++){
			dist[k]=0;
			for(int j=0;j<dimensions;j++){
				dist[k]+=Math.pow((Double.parseDouble(elements[j])-centers.get(j+index)), 2);
			}
			if(dist[k]<min){
				min=dist[k];
				cluster_number=k+1;
			}
			index=index+dimensions;
		}
		// Output the cluster number as key and the point as value, append a 1 to the value
		context.write(new IntWritable(cluster_number), new Text(val+"_1"+""));
	}
}

