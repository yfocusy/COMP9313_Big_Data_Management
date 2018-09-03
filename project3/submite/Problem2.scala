package comp9313.ass3


import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
//import org.apache.log4j.{Level, Logger} 


object Problem2 { 
  
    def main(args: Array[String]) {
        //ignore logs  
        //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)   
          
        val inputFile = args(0)        
        val TARGET = args(1).toLong
        val conf = new SparkConf().setAppName("Problem2").setMaster("local")  
        val sc = new SparkContext(conf)          
        val input = sc.textFile(inputFile)  
 
        /*
         * ------------------------------1. vertexs------------------------------------------
         * 1. Use Array to create vertexs, get all nodes set in the input file
         * 2. data structure is VD:(Long,Double.MaxValue) 
         *  
         */
        val vertexs: RDD[(VertexId, Double)] = input.map{line =>{
            val nodes: Array[String] = line.split("\t| ")
            (nodes(1).toLong, Double.PositiveInfinity)                    
        }
        }.reduceByKey(_+_)        
        val vertexArray = vertexs.collect()
      

        /*
         * ------------------------------2. edges------------------------------------------
         * 1. Use Array to create edges, get each fromNode and it's destNode, 
         *    and then each distance in the input file
         * 2. data structure is ED:Double  
         */
        val edges:RDD[Edge[(Double)]] = input.map{line =>{
            val nodes: Array[String]  = line.split("\t| ")
            Edge(nodes(1).toLong,nodes(2).toLong, (nodes(3).toDouble))                    
        }}
        val edgeArray = edges.collect()

          
        /*
         * ------------------------------3. graph------------------------------------------
         * 1. Create vertexRDD and edgeRdd
         * 2. Create a graph based on vertexRDD and edgeRDD, Graph[VD,ED] 
         */            
        val vertexRDD: RDD[(VertexId, Double)] = sc.parallelize(vertexArray) 
        val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArray) 
        val graph = Graph(vertexRDD, edgeRDD)

        /*
         * ------------------------------4. SSSP------------------------------------------
         * 1. Create a initialGraph to set all distance to infinity except the TARGET Node
         * 2. SSSP algorithm 
         * 
         */       
        val initialGraph = graph.mapVertices((id, _) =>
            if (id == TARGET) 0.0 else Double.PositiveInfinity)
            
                                   
        val sssp = initialGraph.pregel(Double.PositiveInfinity)(
            (id, dist, newDist) => math.min(dist, newDist), // Vertex Program           
            triplet => {                                   // Send Message              
              if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
                Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
              } else {  Iterator.empty  } },             
              (a, b) => math.min(a, b)                // Merge Message
        )
                         
        /*
         * ------------------------------5. final output------------------------------------------
         * 1. use filter to get all nodes which is not the TARGET node and doesn't has Infinity distance
         * 2. count the number
         */ 
        var num = 0         
        sssp.vertices.filter(v=> v._1!=TARGET && v._2 != Double.PositiveInfinity).collect.foreach{
//          v =>println(s"${v._1} =======to==> ${v._2}") 
            v =>(v._1, v._2) 
            num +=1
        }
        println(num)        
    }
}