package Mao_Dilin_tweets

import java.nio.charset.StandardCharsets

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON/**
  * Created by deliamao on 9/16/16.
  */

object TFDF{
  //val shortTwitter = "shortTwitter.txt"
  //val shortTwitter = "dilinoutput.txt"

  // parse Twitter json to object and get text
  def main(args:Array[String]){

    def parseTwitters(line:String): String ={
      val twitter_string = line
      val twitter_json_ob = JSON.parseFull(twitter_string)
      twitter_json_ob match {
        case Some(m: Map[String, String]) => m("text") match {
          case s: String => s
              return s
        }
      }
    }
    //parse word.
    def parseline(line:String):Array[String] ={
      var newline = line
      val utfBytes = newline.getBytes(StandardCharsets.UTF_8)
      newline = new String(utfBytes,"UTF-8")
      //println("raw:" + newline)
      //same as Sentiment.scala
      //clearn the data
      newline = newline.replaceAll("(https|HTTPS|ftp|FTP|http|HTTP)://[\\S]+","")
      newline = newline.replaceAll("(^|\\s)+(RT)?(^|\\s)*@[\\S]+","")
      newline = newline.replaceAll("(^|\\s)+#[\\S]+","")
      //newline = newline.replaceAll("[\\p{Punct}&&[^']]+(\\s|$)", " ")
      newline = newline.replaceAll("[\\p{Punct}]+", "")
      //newline = newline.replaceAll("(^|\\s)[\\p{Punct}&&[^']]+", " ")
      // Removes ' if not in

      //newline = newline.replaceAll("(^|\\s)*'(\\s|$)*", " ")
      newline = newline.replaceAll("[\\s]+", " ")
      newline = newline.replaceAll("^[\\s]+", "")
      newline = newline.replaceAll("[\\s]+$", "")
      newline= newline.toLowerCase
      //println("processed:" + newline)
      val wordsList = newline.split(" ")
       //val wordsList = newline.split(" ")
      return wordsList
    }

    //def replacewithList(key:String,tuple2: Tuple2):(String,Tuple2) ={}


    // spark operation
    val sparkConf = new SparkConf().setMaster("local").setAppName("DilinTest")
    val sc = new SparkContext(sparkConf)
    val input = sc.textFile(args(0))
    // new RDD only include text
    val textRDD = input.map(parseTwitters)
        //textRDD.foreach(println)
    // for test
    val testTextRDD = textRDD.flatMap(parseline)
        //testTextRDD.foreach(println)
    // still need to handle lowcase....UTF-8....
    val idfWithIndexRDD = textRDD.zipWithIndex()
    val idfIndexAsKeyRDD= idfWithIndexRDD.map{case (v,k) => (k+1,v)}
    val idfIndexFlaxMapRDD = idfIndexAsKeyRDD.flatMapValues(parseline).map{case (i,w) => (w,i)}.distinct()
    //idfIndexFlaxMapRDD.sortByKey().foreach(println)
    val idfRDD = idfIndexFlaxMapRDD.map{case (w,i) => (w,1)}.reduceByKey{case (x, y) => x + y}
    // parse word may need to remove
    //textRDD.saveAsTextFile(TFDF_output_File)
    //idfRDD.sortByKey().saveAsTextFile("DF_output")

    // handle the TF
    val textWithIndexRDD = textRDD.zipWithIndex()
    // (list, index) change to Array[(word,index)]
    val tfIndexAsKeyRDD = textWithIndexRDD.map{case (v,k) => (k+1,v)}
    val tfWordandIndexRDD = tfIndexAsKeyRDD.flatMapValues(parseline).map{case (v,k) => (k,v)}.map(word => (word,1)).reduceByKey{case (x, y) => x + y}
    val tfRDD = tfWordandIndexRDD.sortByKey().map{case (k,v) => (k._1,(k._2,v))}
    val tfGroupRDD = tfRDD.groupByKey().map{case (k,list) =>(k,list.toList)}
    val rstRDD = idfRDD.join(tfGroupRDD).sortByKey().map{case (key,(left,right)) => (StringEscapeUtils.escapeJson(key),(left,right))}
    val rst= rstRDD.map{case (key,(left,right)) => s"($key,$left,$right)"}



    //tfGroupRDD.sortByKey().saveAsTextFile("TF_output")
    rst.saveAsTextFile("Mao_Dilin_tweets_tfdf_first20")

  }
}

