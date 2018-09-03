package comp9313.ass1;



import java.io.IOException;
import java.net.URI;
//import java.nio.file.FileSystem;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
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
import org.jboss.netty.util.internal.SystemPropertyUtil;


public class EdgeAvgLen1 {  
	/* 
	 * 1. Mapper: 
	 *    Create EdgeMapper and extends Mapper class. Read the 3rd number as toNode 
	 *    and the 4th number as the distance. Combine the distance and count as a 
	 *    text (toNode, distance\t1) and send it to next step.
	 * 2. Combiner and Reducer: 
	 *    set job.setCombinerClass(EdgeReducer.class) 
	 *    Implement Combiner and Reducer function both in Reducer class.
	 *    For every toNode, Convert each input's value into a textList.
	 *    Initially set the reducerFlag = 0.In this stage, it performs a combiner function.
	 *    When reducerFlag=1, it performs a reducer function.  
	 */
	public static class EdgeMapper extends Mapper<Object, Text, IntWritable, Text>{
		private Double distance = 0.0;
		private Text toNode = new Text();
		private Text distanceAndCount = new Text();
		private String allText = "";
		private String[] textList;
		private int mapcount = 0;
			
   	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   	    	    allText = value.toString();
   		    textList = allText.split("\t | ");
   		    int i = 2;
   		    mapcount = mapcount + 1;
   		    
   		    while (i<textList.length) {
   		    		toNode = new Text (textList[i].toString());
   		    		distance = new Double (textList[i+1].toString());
   		    	    i = i+4; 		    	    
   		    	    distanceAndCount = new Text(distance.toString()+ "\t1");
   		    	    int toNodeint = Integer.parseInt(toNode.toString());
   		    	    IntWritable toNodeInt= new IntWritable(toNodeint);
				context.write(toNodeInt, distanceAndCount);
   			    Log log = LogFactory.getLog(EdgeMapper.class);
   			    log.info("MyLog@Mapper: toNode="+ 	toNode.toString() + " distance="+ distance.toString());
   		      }
   		   }
   	   }// end of map class
	

	public static class EdgeReducer extends Reducer<IntWritable,Text,IntWritable,Text> {	
		
		private DoubleWritable result = new DoubleWritable();
		private Text combinerResult = new Text();
		private String line;
		private Double sum = 0.0;
		private Double tempSum = 0.0;
		private Integer num = 0;
		private Double avg = 0.0;
		
		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			sum = 0.0;
			num = 0;
			avg = 0.0;
			int reduceFlag = 0;
   		    for (Text valAndCount : values) {
   		    	    line = valAndCount.toString();
   		    	    String[] textList = line.split("\t");
   		    	    if (textList.length==3) {
   		    	    		reduceFlag = 1; 	    	
   		    	    }
   		    	    num = num + Integer.parseInt(textList[1]);
   		    	    tempSum =  Double.parseDouble(textList[0]);
   			    sum = sum + tempSum;
   		    }
   		    if (reduceFlag!=1) {
   		    		combinerResult = new Text(sum.toString()+ "\t"+ num.toString() +"\t"+"combined");
   		    		context.write(key, combinerResult);	    	
   		    }else {
   	   		    avg = sum/num;  
   	   		    result.set(avg);
   	   		    
   	   		    context.write(key, new Text(result.toString()));
   	   		    Log log = LogFactory.getLog(EdgeReducer.class);
   	   		    log.info("MyLog@Reducer: "+ key.toString()+ " "+ result.toString());
   		    }
   	    }
     }
  
      public static void run(String inputFile, String outputFile) throws Exception{
   	   Configuration conf = new Configuration();
   	   Job job = Job.getInstance(conf, "edge avg len1");
   	   job.setJarByClass(EdgeAvgLen1.class);
   	   
   	   job.setMapperClass(EdgeMapper.class);
   	   job.setCombinerClass(EdgeReducer.class);
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


