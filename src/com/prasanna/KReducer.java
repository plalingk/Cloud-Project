/*
 * The reducer class is used to collect all the sum_points associated with a cluster center and again add their values 
 * along with the count values. The final sum values are then divided by the count to get the average ie. the centroid 
 * of the cluster. The cluster key and value of centroid is then written to the output.
 */

package com.prasanna;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KReducer extends Reducer <IntWritable,Text,IntWritable,Text>{
	
	// Creating an arraylist to take sum of all the points associated to a key
	List<Double> sum = new ArrayList<Double>();
	
	public void reduce(IntWritable key, Iterable <Text> values, Context context) throws IOException, InterruptedException{
		int count=0;
		String extract;
		String elements[];
		String outvalue ="";
		int i;
		int flag=0;
		// Take a sum of all the points in a cluster - dimension wise
		for(Text str: values){
			extract=str.toString();
			count=count+Integer.parseInt(extract.split("_")[1]);
			elements = extract.split("_")[0].split(",");
			i=0;
			for(String val:elements){
				if(flag==0)
					sum.add(i, (double) 0);
				sum.set(i, sum.get(i)+Double.parseDouble(val));
				++i;
			}
			flag=1;
		}
		// Calculate the cluster centroid by taking the mean of all the points in the cluster
		for(int j=0;j<sum.size();j++){
			sum.set(j, sum.get(j)/count);
			if(outvalue=="")
				outvalue=sum.get(j).toString();
			else
				outvalue=outvalue + "," + sum.get(j).toString();				
		}
		sum.clear();
		// Write the cluster ID as the key and the new calculated centroid as the value
		// This is consumed by the next iteration of the MR algorithm
		context.write(key, new Text(outvalue));
	}
}

