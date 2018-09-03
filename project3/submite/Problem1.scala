package comp9313.ass3

import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Problem1 {
  
    def main(args: Array[String]) {

        val inputFile = args(0)        
        val outputFolder = args(1)
        
        val conf = new SparkConf().setAppName("Problem1").setMaster("local")  
        val sc = new SparkContext(conf)       
        val input = sc.textFile(inputFile)     
        /*
         * 1. get nodes information from the txt file. 
         * 2. convert Nodes to Int. covert distance to Double 
         */
        val nodes_raw = input.map (line => (line.split("\t| ")(1).toInt,line.split("\t| ")(3).toDouble)).groupByKey()
                        
        /*
         * 3. Combine out-going distances for each nodes and compute the average lengths.
         */
        val nodes1 = nodes_raw.map{ x=>(x._1, x._2.reduce(_+_)/x._2.count(x=>true).toDouble)}

        /*
         * 4. order the result first based on average distance ,then nodeId
         * 5. output the final result
         */
        
        val nodes2 = nodes1.sortBy(order => (-order._2, order._1)).map(s => s._1 + "\t" + s._2)
  
        
        nodes2.saveAsTextFile(outputFolder)

    }

}