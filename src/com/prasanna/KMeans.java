/*
 * This is the driver class and contains the code to call both the kmeans and the canopy clustering algorithm.
 * 
 */

package com.prasanna;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KMeans extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(KMeans.class);
	@Override
	public int run(String[] arg) throws Exception {
		
		//If the kmeans algorithm is to be run
		if(arg[2].toLowerCase().equals("kmeans")){
		Path input = new Path(arg[0]);										  // Input Path
		Path output = new Path(arg[1]);										  // Output Path
		Path initial = new Path(arg[3]); 									  // Path to the initial Centroids File
		int maxIter = Integer.parseInt(arg[4]);	
		
		
		Path centroids_file = new Path(initial.toString()+"/kcentroids");
		Path tmp_out = new Path(output + "/cluster-0");
		Path out =null;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		// Adding the initial centroids file to the Distributed Cache
		FileStatus [] fileStatus = fs.listStatus(initial);
		for (FileStatus status: fileStatus){
				DistributedCache.addCacheFile(status.getPath().toUri(), conf);
			}	
		
		// Creating job and setting the parameters for it
		Job km = new Job(conf,"kmeans");
		km.setJarByClass(KMeans.class);
		km.setMapperClass(KMapper.class);
		km.setCombinerClass(KCombiner.class);
		km.setReducerClass(KReducer.class);
		km.setInputFormatClass(TextInputFormat.class);
		km.setMapOutputKeyClass(IntWritable.class);
		km.setMapOutputValueClass(Text.class);
		km.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(km, input);
		FileOutputFormat.setOutputPath(km, tmp_out);
		FileSystem hdfs = FileSystem.get(conf);
		Path success = new Path(tmp_out,"_SUCCESS");
		Path logs = new Path(tmp_out,"_logs");
		
		if(km.waitForCompletion(true))
		{
			hdfs.delete(logs, true);
			hdfs.delete(success, true);
			
			int i=1;			// Iteration number - appended to the name of the cluster output file
			// ********** BEGIN ITERATIVE KMEANS ***************//
			
			while(i<maxIter){
				
				Configuration conf1 = new Configuration();
				fs = FileSystem.get(conf1);
				fileStatus = fs.listStatus(initial);
				for (FileStatus status: fileStatus){
					fs.delete(status.getPath());			// Deleting the previous centroids file
				}
				
				hdfs = FileSystem.get(conf1);
				out = new Path(output+"/"+"cluster-"+(i-1)); // taking the path of the previous cluster
				FileUtil.copyMerge(hdfs, out, hdfs, centroids_file, false, conf1, null); // merging the output to a single folder to be added to the DCache
				
				// Add the file to the DCache
				fs = FileSystem.get(conf1);
				fileStatus = fs.listStatus(initial);
				for (FileStatus status: fileStatus){
					//LOG.info("PATH: " +status.getPath().getName());
					DistributedCache.addCacheFile(status.getPath().toUri(), conf1);
				}
				
				
				// ***** ITERATIVE KMEANS ****** //
				LOG.info("KMeans: Iteration - "+ (i+1));
				Job kmi = new Job(conf1,"KMeans");
				kmi.setJarByClass(KMeans.class);
				kmi.setMapperClass(KMapper.class);
				kmi.setReducerClass(KReducer.class);
				kmi.setInputFormatClass(TextInputFormat.class);
				kmi.setMapOutputKeyClass(IntWritable.class);
				kmi.setMapOutputValueClass(Text.class);
				kmi.setCombinerClass(KCombiner.class);
				kmi.setNumReduceTasks(2);
				FileInputFormat.addInputPath(kmi, input);
				tmp_out = new Path(output+"/"+"cluster-"+i);
				FileOutputFormat.setOutputPath(kmi, tmp_out);
				++i;
				if(kmi.waitForCompletion(true))
				{
					Path success1 = new Path(out,"_SUCCESS");
					Path logs1 = new Path(out,"_logs");
					hdfs.delete(logs1, true);
					hdfs.delete(success1, true);
				}
				else
					return 1;
				
			}
			return 0;	
		}
		else
		{
			return 1;
		}
		}
		
		// If the canopy clustering algorithm is being used
		if(arg[2].toLowerCase().equals("canopy")){
			Path input = new Path(arg[0]);										  // Input Path
			Path output = new Path(arg[1]);										  // Output Path

			// Creating the job and setting the job parameters
			Configuration conf = new Configuration();
			Job canopy = new Job(conf,"canopy");
			canopy.setJarByClass(KMeans.class);
			canopy.setMapperClass(CMapper.class);
			canopy.setReducerClass(CReducer.class);
			canopy.setInputFormatClass(TextInputFormat.class);
			canopy.setMapOutputKeyClass(IntWritable.class);
			canopy.setMapOutputValueClass(Text.class);
			canopy.setNumReduceTasks(1);
			
			FileInputFormat.addInputPath(canopy, input);
			FileOutputFormat.setOutputPath(canopy, output);
			
			if(canopy.waitForCompletion(true)){
				return 1;
			}
			
		}
		return 0;
		
	}
	
	public static void main(String[] arg) throws Exception{
		ToolRunner.run(new Configuration(), new KMeans(), arg);
	}

}

