package com.nits.spark

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.log4j._

//com.nitish24m.spark.TopgearRDD
object TopgearSecond {
  
  def main(args:Array[String]){
    //Set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Creating spark Context
    val sc = new SparkContext("local[*]","TopgearSecond")
    val input = Array(10,21,90,34,40,98,21,44,59,21,90,34,29,19, 21,90,34,29,49,78)
    val file = sc.parallelize(input)
    //file.collect().foreach(println)
    
    val sortedRdd = file.map(x => (x,1)).sortByKey(true,5).map(x => x._1)
    val reverseSortedRdd = file.map(x => (x,1)).sortByKey(false,5).map(x => x._1)
    println("Ascending Order")
    sortedRdd.collect().foreach(x => print(x+" "))  //Ascending Order
    println()
    println("Descending Order")
    reverseSortedRdd.collect().foreach(x => print(x+" "))  //Descending Order
    println()
    val dist = file.distinct()        //RDD with distinct elements
    println("Displying distinct elements of array using RDD:")
    dist.collect.foreach(x => print(x+" "))
    println()
    println("Display distinct elements without using a new RDD.")
    file.distinct().collect.foreach(x => print(x+" "))
  }
  
}