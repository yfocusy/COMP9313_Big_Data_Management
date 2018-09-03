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
   		    System.out.println("mapcount="+mapcount);
   		    System.out.println("textList.length="+textList.length);
   		    
   		    while (i<textList.length) {
   		    		toNode = new Text (textList[i].toString());
   		    		distance = new Double (textList[i+1].toString());
   		    	    i = i+4; 		    	    
   		    	    System.out.println("toNode   =" + toNode.toString());
   		    	    System.out.println("distance =" + distance);
   		    	    distanceAndCount = new Text(distance.toString()+ "\t1");
   		    	    System.out.println("distanceAndCount  =" + distanceAndCount .toString());
   		    	    int toNodeint = Integer.parseInt(toNode.toString());
   		    	    IntWritable toNodeInt= new IntWritable(toNodeint);
				context.write(toNodeInt, distanceAndCount);
   		    	      
   		        System.out.println("----------------- map -----------------");
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
			System.out.println("Reducer 输入分组<" + key.toString() + "  N (N>=1)>");
//			String line = "";
//			double tempSum = 0.0;
//			Double sum = 0.0;
//			Integer num = 0;
//			double avg = 0.0;
//			int reduceFlag = 0;
//			line = "";
//			tempSum = 0.0;
			sum = 0.0;
			num = 0;
			avg = 0.0;
			int reduceFlag = 0;
   		    for (Text valAndCount : values) {
   		    	    line = valAndCount.toString();
   		    	    String[] textList = line.split("\t");
   		    	    System.out.println("textList[0]=" + textList[0]);
   		    	    if (textList.length==3) {
   		    	    		System.out.println("in reducer after=" + textList[2]);
   		    	    		reduceFlag = 1; 	    	
   		    	    }
   		    	    num = num + Integer.parseInt(textList[1]);
   		    	    tempSum =  Double.parseDouble(textList[0]);
   			    sum = sum + tempSum;
   		    }
   		    if (reduceFlag!=1) {
   		    		combinerResult = new Text(sum.toString()+ "\t"+ num.toString() +"\t"+"combiner");
   		    		context.write(key, combinerResult);	    	
   		    }else {
   	   		    avg = sum/num;  
   	   		    System.out.println("key="+ key.toString() + " avg="+avg + " num="+num);
   	   		    result.set(avg);
   	   		    //key.toString().
   	   		    
   	   		    context.write(key, new Text(result.toString()));
   	   		    System.out.println("----------------- reduce -----------------");
   	   		    System.out.println(key.toString()+" "+result.toString());
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
   	   Configuration conf = new Configuration();
   	   run(INPUT, OUTPUT);
   	   
   	   
   	   
   	   
   	 
      }

}


