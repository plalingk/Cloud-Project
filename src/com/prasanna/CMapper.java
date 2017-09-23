/*
 * This class is the mapper for the canopy clustering algorithm. The setup method initializes the list needed to store all the
 * data. The map function populates this static list of datapoints. In the cleanup method we perform the actual canopy clustering
 * steps. This class is used when we choose the initial center of the canopy as the canopy centroid.
 */

package com.prasanna;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CMapper extends Mapper<LongWritable,Text,IntWritable,Text> {

	// Needed class variables
	private static final Log LOG = LogFactory.getLog(KMapper.class);
	int num_centers = 0;
	int dimensions = 0;
	public static ArrayList<ArrayList<Double>> dataPoints;
	
	// Initialize the static datapoints list in the setup method
	public void setup(Context context) throws IOException{
		dataPoints = new ArrayList<ArrayList<Double>>();
	}
	
	// Populate the datapoints list by reading all the datapoints in the input split of the mapper
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		ArrayList<Double> point = new ArrayList<Double>();
		String elements[] = values.toString().split(",");
		
		for(int i = 0; i<elements.length; i++){
			point.add(Double.parseDouble(elements[i]));
		}
		dataPoints.add(point);
	}
	
	// Calculte the canopies for the dataset and output the canopies
	public void cleanup(Context context) throws IOException, InterruptedException{
		int randomPosition;
		ArrayList<Double> canopyCenter = null;
		ArrayList<ArrayList<Double>> canopyPoints = new ArrayList<ArrayList<Double>>();
		float distance;
		float t1 = 100, t2 = 10;
		
		while(dataPoints.size() > 0){
			distance = 0;
			
			// Setting a point at random as a canopy center and removing it from the set
			randomPosition = new Random().nextInt(dataPoints.size());
			canopyCenter = new ArrayList<Double>(dataPoints.get(randomPosition));
			canopyPoints.add(canopyCenter);			
			dataPoints.remove(randomPosition);					
			
			for(int i = 0; i < dataPoints.size(); ){				
				int flag = 0;
				// Clustering all remaining points around that canopy
				double sum = 0;
				for(int k = 0; k < dataPoints.get(i).size(); k++){
					sum = sum + Math.pow(Math.abs(dataPoints.get(i).get(k) - canopyCenter.get(k)), 2);
				}
				distance = (float) Math.sqrt(sum);
				if(distance < t1){
					if(distance < t2){
						dataPoints.remove(i);
						flag = 1;
					}
				}
				if(flag!=1){
					i++;
				}
			}
		}	
		
		if(canopyPoints.size()>0){
			int index = canopyPoints.size()-1;
			canopyPoints.remove(index);
		}
		
		
		//Write the canopy cluster center to output
		for(int i=0; i<canopyPoints.size();i++){
			StringBuilder sb = new StringBuilder();
			canopyCenter.clear();
			canopyCenter = canopyPoints.get(i);

			for(int j = 0; j<canopyCenter.size(); j++){
				if(j!=canopyCenter.size()-1){
					sb.append(Double.toString(canopyCenter.get(j))+",");					
				}
				else{
					sb.append(Double.toString(canopyCenter.get(j)));
				}
			}
			// Write the canpy center as value and a dummy key as the key to output
			context.write(new IntWritable(1), new Text(sb.toString()));
		}
		
	}
	
	
}

