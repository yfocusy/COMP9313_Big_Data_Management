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
	public static class EdgeMapper extends Mapper<Object, Text, IntWritable, Text>{
		private Double distance = 0.0;
		private Text toNode = new Text();
		private Text distanceAndCount = new Text();
		private String allText = "";
		private String[] textList;
		
   	    private Map<IntWritable, ArrayList<Double>> map = new HashMap<IntWritable, ArrayList<Double>>();
   	    //private ArrayList<Double> distanceList = new ArrayList<Double>();
   	    //private ArrayList<Double> distanceList = new ArrayList();
   	    
   	    

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 	    	
   	    		allText = value.toString();
   		    textList = allText.split("\t | ");
   		    int i = 2;
   		    while (i<textList.length) {
   		    		toNode = new Text (textList[i].toString());
	   		    	int toNodeint = Integer.parseInt(toNode.toString());
	   		    	IntWritable toNodeInt= new IntWritable(toNodeint);
   		    		distance = new Double (textList[i+1].toString());
   		    	    System.out.println("toNodeint   =" + toNodeint);
   		    	    System.out.println("distance =" + distance);
   		    	    
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
   	    			System.out.println();
   	    			System.out.println("combiner node="+node);
   	    			System.out.println("dislist.len="+ disList.size());
   	    			System.out.println();
   	    			for (Double e: disList) {
   	    				System.out.print(e+" ");
   	    			}
   	    			Double temp_sum = 0.0;
   	    			for (Double eachDis : disList) {
   	    				temp_sum = temp_sum + eachDis;  	    				
   	    			}
   	    			Integer length = disList.size();	
   	    			//System.out.println("xxx="+temp_sum.toString() + "_"+length.toString());
   	    			Text sumAndlength = new Text(temp_sum.toString() + "_"+length.toString());
   	    			//Text sumAndlength = new Text(temp_sum.toString());
   	    			System.out.println("sumAndlength="+sumAndlength);
   	    			
   	    			//context.write(node,sumAndlength); 
   	    			
   		    	    int toNodeint = Integer.parseInt(node.toString());
   		    	    IntWritable NodeInt= new IntWritable(toNodeint);
				//context.write(NodeInt, distanceAndCount);
				context.write(NodeInt, sumAndlength);
   	    		}
   	    }// end of cleanup
   	   }// end of map class

	
	
	

	public static class EdgeReducer extends Reducer<IntWritable, Text,IntWritable,DoubleWritable> {	
		
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
			System.out.println("key="+key);			
   		    for (Text valAndCount : values) {
   		    	    line = valAndCount.toString();
   		    	    System.out.println("line="+line);
   		    	    String[] textList = line.split("_");
   		    	    System.out.println("textList[0]=" + textList[0]);
   		    	    num = num + Integer.parseInt(textList[1]);
   		    	    tempSum =  Double.parseDouble(textList[0]);
   			    sum = sum + tempSum;
   		    }

   		    avg = sum/num;  
   		    System.out.println("key="+ key.toString() + " avg="+avg + " num="+num);
   		    result.set(avg);
   		    context.write(key, result);
   		    
   		    System.out.println("----------------- reduce -----------------");
   		    System.out.println(key.toString()+" "+result.toString());
   		    Log log = LogFactory.getLog(EdgeReducer.class);
   		    log.info("MyLog@Reducer: "+ key.toString()+ " "+ result.toString());
	    
   	    }
     }

      
      
      
      public static void run(String inputFile, String outputFile) throws Exception{
   	   Configuration conf = new Configuration();
   	   Job job = Job.getInstance(conf, "edge avg len2");
   	   job.setJarByClass(EdgeAvgLen2.class);
   	   
   	   job.setMapperClass(EdgeMapper.class);
   	   //job.setCombinerClass(EdgeReducer.class);
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
//   	   Path input_file = new Path(INPUT);
////   	   FileSystem fs = FileSystem.get(URI.create(url_1+"/1.txt"),conf);
//   	   FileSystem fs = FileSystem.get(conf);
//   	   
//   	   fs.append(new Path("/user/yuli510/2.txt"));
//   	   System.out.println();
   	   run(INPUT, OUTPUT);
   	   
   	   
   	   
   	   
   	 
      }

}


