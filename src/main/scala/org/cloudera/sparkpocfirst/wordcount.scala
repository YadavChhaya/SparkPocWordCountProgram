package org.cloudera.sparkpocfirst

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordcount {
    
  def main(args: Array[String]):Unit = {
    
    val conf = new SparkConf().setAppName("Wordcount").setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.textFile("/home/acadgild/inputwc.txt")
    val rdd2 = rdd1.flatMap{_.split(" ")}.map(word=>(word,1)).reduceByKey(_+_)
    rdd2.foreach(println)
    
    val rdd3 = sc.parallelize(Seq("Rashi","Ritu","Varsha","Rashi","Ritu"))
    val rdd4 = rdd3.flatMap{x => x.split(",")}.countByValue()
    rdd4.foreach(println)
    sc.stop()
    
  }
}