package comp9313.ass1;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
//import java.nio.file.FileSystem;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;

public class EdgeAvgLen2 {  
	/* 
	 * 1. Create EdgeMapper and extends Mapper class. Read the 3rd number as toNode 
	 *    and the 4th number as the distance.
	 * 2. Use a HashMap to store (node, distance array) pair
	 * 3. user cleanup to do the In-mapper combiner. 
	 *    Each node has a array which store all distance value coming to it. 
	 *    In cleanup function, sum all values of distances and keep the count
	 *    of how many arrows coming to this node into a text (sum_num). 
	 *    Pass (node, sum_num) to reducer.
	 * 4. Compute the average of each node in reducer and output the result.  
	 */
	public static class EdgeMapper extends Mapper<Object, Text, IntWritable, Text>{
		private Double distance = 0.0;
		private Text toNode = new Text();
		private String allText = "";
		private String[] textList;	
   	    private Map<IntWritable, ArrayList<Double>> map = new HashMap<IntWritable, ArrayList<Double>>();
   	    
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 	    	
   	    		allText = value.toString();
   		    textList = allText.split("\t | ");
   		    int i = 2;
   		    while (i<textList.length) {
   		    		toNode = new Text (textList[i].toString());
	   		    	int toNodeint = Integer.parseInt(toNode.toString());
	   		    	IntWritable toNodeInt= new IntWritable(toNodeint);
   		    		distance = new Double (textList[i+1].toString());   		    	    
   		    		if (!map.containsKey(toNodeInt)) {
   		    			ArrayList<Double> tmpList = new ArrayList<Double>();
   		    			tmpList.add(distance);
   		    			map.put(toNodeInt, tmpList);
   		    		}else {
   		    			ArrayList<Double> tmpList = map.get(toNodeInt);
   		    			tmpList.add(distance);
   		    			map.put(toNodeInt, tmpList);
   		    		}
   		    	    i = i+4;  		    	    
   		    }
   		}
   	    
   	    public void cleanup(Context context) throws IOException, InterruptedException{
   	    		Set<Entry<IntWritable,ArrayList<Double>>> sets = map.entrySet();
   	    		for(Entry<IntWritable,ArrayList<Double>> entry: sets){
   	    			IntWritable node = entry.getKey();
   	    			ArrayList<Double> disList = entry.getValue();
   	    			Double temp_sum = 0.0;
   	    			for (Double eachDis : disList) {
   	    				temp_sum = temp_sum + eachDis;  	    				
   	    			}
   	    			Integer length = disList.size();	
   	    			Text sumAndlength = new Text(temp_sum.toString() + "_"+length.toString());   	    			
   		    	    int toNodeint = Integer.parseInt(node.toString());
   		    	    IntWritable NodeInt= new IntWritable(toNodeint);
				context.write(NodeInt, sumAndlength);
   	    		}
   	    }// end of cleanup
   	   }// end of map class


	public static class EdgeReducer extends Reducer<IntWritable, Text,IntWritable,DoubleWritable> {	
		
		private DoubleWritable result = new DoubleWritable();
		private String line;
		private Double sum = 0.0;
		private Double tempSum = 0.0;
		private Integer num = 0;
		private Double avg = 0.0;
		
		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			sum = 0.0;
			num = 0;
			avg = 0.0;			
   		    for (Text valAndCount : values) {
   		    	    line = valAndCount.toString();
   		    	    String[] textList = line.split("_");
   		    	    num = num + Integer.parseInt(textList[1]);
   		    	    tempSum =  Double.parseDouble(textList[0]);
   			    sum = sum + tempSum;
   		    }

   		    avg = sum/num;  
   		    result.set(avg);
   		    context.write(key, result);
   		    Log log = LogFactory.getLog(EdgeReducer.class);
   		    log.info("MyLog@Reducer: "+ key.toString()+ " "+ result.toString());	    
   	    }
     }

      
            
      public static void run(String inputFile, String outputFile) throws Exception{
	      Configuration conf = new Configuration();
	      Job job = Job.getInstance(conf, "edge avg len2");
	      job.setJarByClass(EdgeAvgLen2.class);
	   
	      job.setMapperClass(EdgeMapper.class);
	      job.setReducerClass(EdgeReducer.class);
	   
	      job.setMapOutputKeyClass(IntWritable.class);
	      job.setMapOutputValueClass(Text.class);
	   
	      job.setOutputKeyClass(IntWritable.class);
	      job.setOutputValueClass(DoubleWritable.class);
	   
	      FileInputFormat.addInputPath(job, new Path(inputFile));
	      FileOutputFormat.setOutputPath(job, new Path(outputFile));
	      System.exit(job.waitForCompletion(true) ? 0 : 1);
	   
      }

      public static void main(String[] args) throws Exception {
   	      String INPUT  = args[0];
   	      String OUTPUT = args[1];
   	      run(INPUT, OUTPUT); 	 
      }

}


