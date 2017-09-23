/*
 * The combiner class takes mapper output ie. all points corresponding to a center and takes a sum of the individual dimension. 
 * It also maintains a count (1 was added to the mapper output) so that taking average while finding the center in the reducer  
 * is possible. It creates the sum of all the vectors and writes it to the output as text and concatenated count.
 */

package com.prasanna;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class KCombiner	extends Reducer<IntWritable,Text,IntWritable,Text> {

	// Creating an arraylist to take sum of all the points associated to a key
	List<Double> sum = new ArrayList<Double>();
	
	public void reduce(IntWritable key, Iterable <Text> values, Context context) throws IOException, InterruptedException{
		int count =0;
		String outvalue = "";
		String extract;
		String elements[];
		int flag =0;
		sum.clear();
		int i;		
		for (Text str : values){
			extract = str.toString();
			count = count+Integer.parseInt(extract.split("_")[1]);
			elements = extract.split("_")[0].split(",");
			i=0;
			for(String val : elements) {
				if(flag==0)	// if first value, initialize 0 in the arraylist and then add 
				    sum.add(i, (double) 0);
				sum.set(i, sum.get(i)+Double.parseDouble(val));
				++i;
			}
			flag =1;	
		}
		// take a sum of all the points associated with the cluster center
		for(int j=0;j<sum.size();j++)
	    {
	       if(outvalue=="")
	    	outvalue=sum.get(j).toString();
	    	else
	    	  outvalue=outvalue + "," + sum.get(j).toString();
	    			    	
	    }
		outvalue=outvalue+"_"+count+"";
		// write the sum of all points and the count concatenated to it
		context.write(key, new Text(outvalue));
		
	}
}

