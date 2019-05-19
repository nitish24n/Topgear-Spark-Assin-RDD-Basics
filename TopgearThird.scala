package com.nits.spark

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.log4j._

//com.nitish24m.spark.TopgearRDD
object TopgearThird {
  
  def main(args:Array[String]){
    //Set the log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Creating spark Context
    val sc = new SparkContext("local[*]","TopgearThird")
    val input = Array(10,21,90,34,40,98,21,44,59,21,90,34,29,19, 21,90,34,29,49,78)
    val file = sc.parallelize(input,5)
    val max = file.max()                //max elememt
    val min = file.min()                //min element
    println("Maximum element in Array : "+max)
    println("Minimum element in Array : "+min)
    
    val fiveElem = file.take(5)
    println("First five elements :")
    fiveElem.foreach(x => print(x+"  "))
    println()
    
    val newInput = Array(30,35,45,60,75,85)
    val combinedArr = input ++ newInput  //combining arrays
    println("Combined array : ")
    combinedArr.foreach(x => print(x+"  "))
    println()
    
    val rdd2 = sc.parallelize(combinedArr, 5)
    val distinct = rdd2.distinct()      //distinct elements rdd
    println("distinct elements : ")
    distinct.collect.foreach(x => print(x+"  "))
    println()
    
    println("Sum of distict elements : ")
    //val sum = distinct.sum()
    val sum = distinct.reduce((x,y) => x+y)    //sum of distinct elements
    println(sum)
    
    println("Sum of array elements using reduce")
    val sum2 = rdd2.reduce((x,y) => x+y)      //sum of all the elements of array
    println(sum2)
    
  }
  
}