/**
  * Created by deliamao on 10/7/16.
  */

import org.apache.spark.SparkConf

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by deliamao on 10/7/16.
  */


object dilin_mao_spark{
  def main(args:Array[String]){
    def parseline(line:String):(String,String) ={
      var newline = line
      newline = newline.replaceAll("^[(]","")
      newline = newline.replaceAll("[)]$","")
      if (newline contains "', '"){
        newline = newline.replaceAll("', '", "'\", \"'")
      }else if(newline contains "\", '"){
        newline = newline.replaceAll("\", '", "\"\", \"'")
      }else if(newline contains "', \""){
        newline = newline.replaceAll("', \"", "'\", \"\"")
      }else if(newline contains "\", \""){
        newline = newline.replaceAll("\", \"", "\"\", \"\"")
      }
      val content = newline.split("\", \"")
      val key = content(0)
      val value = content(1)
      return (key,value)
    }

    def getPair(line:List[String]):List[Tuple2[String,String]]={
      var list = List[Tuple2[String,String]]()
      val newline = line.sorted
      //val newline = line
      for(i <- 0 until newline.length){
        for(j <- (i + 1) until newline.length){
          list ::= (newline(i),newline(j))

        }
      }
      return list
    }

    val sparkConf = new SparkConf().setMaster("local").setAppName("DilinTest")
    val sc = new SparkContext(sparkConf)
    val input_one = sc.textFile(args(0))
    val input_two = sc.textFile(args(1))
    val support = (args(2)).toInt
    //val support = 7
    //val input_one = sc.textFile("actress")
    //val input_two = sc.textFile("director")

    val input_one_dist = input_one.distinct()
    val input_two_dist = input_two.distinct()
    val input_one_cln = input_one_dist.map(parseline)
    val input_two_cln = input_two_dist.map(parseline)
    val act_plus_direct = input_one_cln.join(input_two_cln).map({case(k,v) => v})
                          .map(pair => (pair,1)).reduceByKey{case (x, y) => x + y}
                          .filter({case(x,v) => v >= support})

    val act_act = input_one_cln.groupByKey().map({case(k,v) => (k,v.toList)})
                  .map({case(k,v) => v}).filter(line => line.length >1)
                  .flatMap(getPair).map(pair => (pair,1)).reduceByKey{case (x, y) => x + y}
                  .filter({case(x,v) => v >= support})
    val di_di = input_two_cln.groupByKey().map({case(k,v) => (k,v.toList)})
                    .map({case(k,v) => v}).filter(line => line.length >1)
                    .flatMap(getPair).map(pair => (pair,1)).reduceByKey{case (x, y) => x + y}
                    .filter({case(x,v) => v >= support})

    val union_data_one = act_plus_direct.union(act_act)
    val union_data = union_data_one.union(di_di).sortBy(_._2)
    //val union_data = union_data_one.union(di_di).reduceByKey{case (x, y) => x + y}
                     //.filter({case(x,v) => v >=support}).sortBy(_._2)
    val newrst = union_data.repartition(1)

    /*
    val union_data = input_one_dist.union(input_two_dist)
    val whole_data = union_data.map(parseline).groupByKey().map({case(k,v) => (k,v.toList)})
    val target_data = whole_data.map({case(k,v) => v}).filter(line => line.length >1)
    val pair_data = target_data.flatMap(getPair).map(pair => (pair,1)).reduceByKey{case (x, y) => x + y}
    val result_data = pair_data.filter({case(x,v) => v >= support})
    val rst=result_data.sortByKey().sortBy(_._2) */



    //val whole_data_collect = whole_data.collect
    //val removeRDD = whole_data_collect.map(parseline)
    //target_data.foreach(println)

    //removeRDD.foreach(println)
    //val newrst = rst.repartition(1)
    //val tak_data = whole_data.take(6)
    newrst.saveAsTextFile("dilin_mao_spark")

  }

}