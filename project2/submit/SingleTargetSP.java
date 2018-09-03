package comp9313.ass2;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class SingleTargetSP {


    public static String OUT = "output";
    public static String IN = "input";
	static enum iterations{
		numIteration
	}		
	/*
	 * -------------1. Init format -------------------------------------------------------------------------------------------
	 * 1. Input: 
	 *    Use the initial input graph file to generate our desired format
	 * 2. Mapper:
	 *    generate one Node and each its neighbour to this Node's distance 
	 * 3. Reducer:
	 *    processes the initial distance from each Node to the TARGET node's distance ( either 0 or INF).
	 *    Then generate each Node's neighbourList  
	 * 4. Output: 
	 *    Format = Node,  minDistance to TARGET node , neighbourList ( FromNode1:Distance1, FromNode2:Distance2, ......)	
	 */
	public static class InitMapper extends Mapper<Object, Text, LongWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	    String[] lineList = value.toString().split(" |\t");
        	    String fromNode = lineList[1];
        	    String toNode   = lineList[2];
        	    String distance = lineList[3];
        	    context.write(new LongWritable(Long.parseLong(toNode)), new Text(fromNode + ":" + distance));
        }		
	}
	
	public static class InitReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        		String TARGET = context.getConfiguration().get("TARGET");
        		String outValue = "";
        		
        		if(TARGET.equals(key.toString())) {
        			outValue = "0" + "\t";
        		}else {
        			outValue = "INF" + "\t";
        		}
        		ArrayList<Text> neighbours = new ArrayList<Text>(); 		
            for (Text value: values) {
	        		if(!neighbours.contains(value)) {
	        			Text ele = new Text(value.toString());
	        			neighbours.add(ele);
	        			outValue = outValue + value.toString() + ",";
	        		} 		
        		}
            context.write(key, new Text(outValue)); 
        }		
	}
	

    /*
     * -------------2. ST -------------------------------------------------------------------------------------------
     * 1. Input: 
     *    Use the Init output0 as the input
     * 2. STMapper:
     *    During each loop generate format = Node   currentminDistance    neighbourList    path 
     *    not only for the input keyNode, but also nodes in neighbourList
     * 3. STReducer:
     *    During each loop compute the shortest distance and path base on the values.
     * 4. Output:
     *    Format = Node, minDistance to TARGET node, neighbourList ( FromNode1:Distance1, FromNode2:Distance2, ......) shorestPath  possibleDistance1:path1    possibleDistance2:path2 ....
     * 5. When to stop:
     *    The while loop in the main function will call this second MapReduce.
     *    When there is a new shortest distance and path found, the numIteration of be add 1.
     *    When there is no changes found for the shortest distance and path, the while loop will stop.          
     */
	public static class STMapper extends Mapper<Object, Text, LongWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
    			
        	    String[] lineList = value.toString().split(" |\t");
            	String TARGET = context.getConfiguration().get("TARGET");
            	String outValue = "";

            	String path = TARGET;
    	        String keyNode = lineList[0];
            	String minDistance = lineList[1];
    	        String neighbour = lineList[2];
    	            	           	        	    	        
    	        if(context.getConfiguration().getInt("run", 0) == 1) {
    	        		path = TARGET;
        	        outValue = minDistance + "\t" + neighbour + "\t" + path;   	    
//    	        }else if (context.getConfiguration().getInt("run", 0) > 1) {
    	        }else {   
    	        		path = lineList[3];
    	        		outValue = minDistance + "\t" + neighbour + "\t" + path;
    	        }

    	        context.write(new LongWritable(Long.parseLong(keyNode)), new Text(outValue)); 
    	        
    	        
    	        String[] neighbourList = neighbour.split(",");

    	        for (String pair : neighbourList) {
    	        	    if(pair.equals("null")) { 
    	        	    	    continue; 
    	        	    	}
	        		String neighbourFrom = pair.split(":")[0];
	        		String neighbourDistance = pair.split(":")[1];
	        		if(!minDistance.equals("INF") && !minDistance.equals("1.7976931348623157E308")) {
	        			double newDistance = Double.parseDouble(minDistance) + Double.parseDouble(neighbourDistance);
	        			String newPath = neighbourFrom + "->" + path;
	        			outValue = Double.toString(newDistance) +"\t" + newPath;
	        			context.write(new LongWritable(Long.parseLong(neighbourFrom)), new Text(outValue));
	        		}
    	        }   	             
        }
    }// enf of STMapper
	    
    public static class STReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	    String outValue = "";
            double minDistance = Double.MAX_VALUE;
            double curDistance = Double.MAX_VALUE;
            String neighbourAndDistance = null;
            String path = null;
            
            for (Text value: values) {
            		String[] valueList = value.toString().split(" |\t");
            		double distance;   
        			if(valueList[0].equals("INF")) {
        				distance = Double.MAX_VALUE;
        			}else {
        				distance = Double.parseDouble(valueList[0]);
        			}
        			
        			
            		if(valueList.length == 3) {
            			curDistance = distance; 
            			if(curDistance <= minDistance) {
            				minDistance = curDistance; 
            				path = valueList[2];
            			}
            			neighbourAndDistance = valueList[1];
            		}else if (valueList.length == 2) {
            			distance = Double.parseDouble(valueList[0]);
            			if(distance <= minDistance) {
            				minDistance = distance;
            				path = valueList[1];   				
            			}
            		}
            }// end of for loop
            
            if(minDistance < curDistance) {
            		context.getCounter(iterations.numIteration).increment(1);
            }
            
            outValue = Double.toString(minDistance) + "\t" + neighbourAndDistance + "\t" + path;
            context.write(key, new Text(outValue));       
        }
    }// end of STReducer
 
      
    /*
     * -------------3. Final format -------------------------------------------------------------------------------------------	 
     * 1. Input:
     *    the last output of STReducer which contains the shortest distance and path
     * 2. Mapper:
     *    Only keep the shortest distance which is not INF to TARGET node.
     * 3. Reducer:
     *    Generate our desired output format.
     * 4. Output:
     *    Format = Node,  shortest distance, path to the TARGET node
     */
    public static class FinalMapper extends Mapper<Object,Text, LongWritable, Text>{
    		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    			String[] valueList= value.toString().split(" |\t");
    			String keyNode = valueList[0];
    			double distance = Double.parseDouble(valueList[1]);
    			String path = valueList[3];
    			String outValue = distance + "\t"+ path;
    			if(distance!=Double.MAX_VALUE) {
    				context.write(new LongWritable(Long.parseLong(keyNode)), new Text(outValue));	
    			}	
    		}	
    	}// end of FinalMapper
    
    
    public static class FinalReducer extends Reducer<LongWritable, Text, LongWritable,Text>{
    	    @Override
    		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        		for (Text value: values) {
        			context.write(key,value);   	
        		}
    		}
    }// end of FinalReducer

    public static void getInitFormat(String input,String output , String arg) throws Exception{  
		Configuration conf = new Configuration();
		conf.set("TARGET", arg);
		Job job = Job.getInstance(conf, "Init format");
		
		job.setJarByClass(SingleTargetSP.class);
		job.setMapperClass(InitMapper.class);
		job.setReducerClass(InitReducer.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
		
    }
    
	public static void getFinalResult(String input, String output) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "FinalResult");
		
		job.setJarByClass(SingleTargetSP.class);
		job.setMapperClass(FinalMapper.class);
		job.setReducerClass(FinalReducer.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);

	}

    public static void main(String[] args) throws Exception {        

        IN = args[0];
        OUT = args[1];
        int iteration = 0;
        String input = IN;
        String output = OUT + iteration;
        long count  = 1;
        Configuration conf = new Configuration();
        FileSystem fs =  FileSystem.get(new URI(OUT),conf);
        
     /*
      * 1. STEP1: get the right format of the input file
      */
        getInitFormat(input,output,args[2]);
        
     /*
      * 2. STEP2: loop for shortest path            
      */
        iteration = iteration + 1;
        input = output;
        output = OUT + iteration;
		boolean isdone = false;              
        while (isdone == false) {
			conf.set("TARGET", args[2]);
			conf.setInt("run", iteration);
        		Job job = Job.getInstance(conf,"ShortestPath");
    		    job.setJarByClass(SingleTargetSP.class);
    		    job.setMapperClass(STMapper.class);   
    		    job.setReducerClass(STReducer.class);
    		    
    		    job.setOutputKeyClass(LongWritable.class);
    			job.setOutputValueClass(Text.class);
    			    			
    			FileInputFormat.addInputPath(job, new Path(input));
    			FileOutputFormat.setOutputPath(job, new Path(output));	
    			job.waitForCompletion(true); 
    			

    			if(iteration>0){
    				Path tmp_path = new Path(OUT+(iteration-1));
    				if(fs.exists(tmp_path)){
    					fs.delete(tmp_path,true);
    				}
    			}
    			
    			
    			input = output;
    			iteration ++;
    			output = OUT +  iteration;
    			
    			// Check the changed numIteration
    			count = job.getCounters().findCounter(iterations.numIteration).getValue();
    			if (count == 0) {
    				isdone = true;
    			}    		
        }    
               
        /*
         * 3. STEP3: output final result
         */
        getFinalResult(input, OUT);     
		fs.delete(new Path(OUT + (iteration -1)),true);
		System.exit(1);
    }
}

