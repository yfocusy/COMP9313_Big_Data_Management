package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast 
import java.lang.Math.ceil
object SetSimJoin {
  
    def main(args: Array[String]) {
      
        val inputFile = args(0)        
        val outputFolder = args(1)
        val t = args(2).toDouble
//        println("t = "+ t)
              
        val conf = new SparkConf().setAppName("SetSimJoin")
        val sc = new SparkContext(conf)       
        val recordsRaw = sc.textFile(inputFile).map(line=>line.split("\t| ").map(_.toInt)).map(line => (line(0),line.drop(1)))
        recordsRaw.persist()

/*------------------------------Stage 1: Ordering input ----------------------------------  
 * 1. User tokens frequency to make input raw RDD ordered
 * 2. When tokens have same frequency value, token will be ordered by each Int value from small to large.
 */
        val popularOrder = sc.broadcast(recordsRaw.map(x=>x._2).flatMap { x => x }.map(x=>(x,1)).reduceByKey(_+_).collect.toMap)

        val recordOrdered = recordsRaw.map(x=>(x._1,x._2.sortWith((x,y)=>x.toInt<y.toInt).sortBy(e=>popularOrder.value(e))))
       

/*------------------------------Stage 2: Processing prefix ----------------------------------
 * 1. Find prefix value for each record following the formula : P = |r| - ceil(|r| * t) + 1
 * 2. Yield (CommonString, rid1+record1) pairs for each prefix string
 * 3. All these pairs are stored in prefixRecordMap RDD.
 */
        val prefixRecordMap = recordOrdered.flatMap{
              line=>
                  val id = line._1
                  val recordArray = line._2
                                                
                  val p = recordArray.length - ceil(recordArray.length*t).toInt + 1
                  val eachPrefixList = recordArray.take(p)
                  for (i<- 0 to eachPrefixList.length-1) yield(eachPrefixList(i),Array(id)++recordArray)                                
        }
/*------------------------------Stage 3: Processing prefix ----------------------------------  
 * 1. Self join with rdds
 * 2. Filter the all candidates to meet the id constrain whose rid1 should less than rid2. 
 * 3. Filter with the length constrains of the record2 to meet the requirement that: 
 *    |record2| >= |record1|  * t 
 * 4. After remove impossible candidates pairs, Jaccard similarity computation start. 
 * 5. Filter out results which meet the threshold requirement. 
 * 6. Reduce the duplicated result to one. 
 * 7. Sort the result.                  
 */      
        val joinRecord = prefixRecordMap.join(prefixRecordMap)
                         .filter{e=> e._2._1(0) < e._2._2(0)}
                         .filter{k=> k._2._1.drop(1).length*t<= k._2._2.drop(1).length}
 

        val filterRecord = joinRecord.map{
            e=>
                val id1 = e._2._1(0)
                val id2 = e._2._2(0)
                val record1 = e._2._1.drop(1)
                val record2 = e._2._2.drop(1)
                val union = (record1 union record2).distinct
                val intersect = record1 intersect record2 
                val jaccardValue = intersect.length.toDouble / union.length.toDouble 
                ((id1,id2),jaccardValue)               
        }.filter(e=>e._2>=t).reduceByKey((pre,after)=>pre).sortByKey()
            
             
//        println("filterRecord.len = " + filterRecord.collect().length)
        val output = filterRecord.map(s => s._1 + "\t" + s._2)
        output.saveAsTextFile(outputFolder)

    }// end of main
}