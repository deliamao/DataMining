package Mao_Dilin_tweets

import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by deliamao on 9/11/16.
  */

object Sentiment {
  //val testSample = "AFINN-111.txt"
  //val shortTwitter = "wlloutput.txt"
  //val shortTwitter = "shortTwitter.txt"
  //val outputFileTwo ="outputTwo"
  var sentimentMap:Map[String,Int] = Map()
  def main(args:Array[String]){
    //store the AFINN-111 file in a dictionary
    for(line <- Source.fromFile(args(1)).getLines()){
      var list = line.split('\t')
      var word = list(0)
      //println("w:" + word)
      //println("" + list(1))
      var score = list(1).toInt
      //println(score)
      sentimentMap += (word -> score)
      //println(sentimentMap(word))
    }

    // parse Json Ojbect, get out the text value
    def parseTwitters(line:String): String ={
      val twitter_string = line
      val twitter_json_ob = JSON.parseFull(twitter_string)
      twitter_json_ob match {
        case Some(m: Map[String, String]) => m("text") match {
          case s: String => s
            //println("read text:" + s)
            return s
        }
      }
    }

    // clearn the data
    def getTwitterScore(line:String):(Int)={
      var sum =0
      var newline = line
      val utfBytes = newline.getBytes(StandardCharsets.UTF_8)
      newline = new String(utfBytes,"UTF-8")
      //println("raw:" + newline)
      // remove the url
      newline = newline.replaceAll("(https|HTTPS|ftp|FTP|http|HTTP)://[\\S]+","")
      //remove the hashtag @ and prefix
      newline = newline.replaceAll("(^|\\s)+(RT)?(^|\\s)*@[\\S]+","")
      // remove the hashtag #
      newline = newline.replaceAll("(^|\\s)+#[\\S]+","")
      //newline = newline.replaceAll("[\\p{Punct}&&[^']]+(\\s|$)", " ")
      // remove the ASCII punct
      newline = newline.replaceAll("[\\p{Punct}]+", "")
      //newline = newline.replaceAll("(^|\\s)[\\p{Punct}&&[^']]+", " ")
      // Removes ' if not in
      //newline = newline.replaceAll("(^|\\s)*'(\\s|$)*", " ")
      // remove some unappropriate space
      newline = newline.replaceAll("[\\s]+", " ")
      newline = newline.replaceAll("^[\\s]+", "")
      newline = newline.replaceAll("[\\s]+$", "")
      newline= newline.toLowerCase
      //println("processed:" + newline)

      var wordsList = newline.split(" ")
      for(word <- wordsList){
        var tmp = word
          //println(tmp)
        if(sentimentMap.contains(tmp)){
           sum += sentimentMap(tmp)
        }
      }
      return sum
    }
      //println(sentimentMap("abhorrent"))
    // println( getTwitterScore("At Christmas https://drive.google.com/drive/u/1/my-drive I no more desire a rose Than wish a snow in May’s new-fangled mirth."))
    //var testTwitter = "@begin RT @dilinmao RT@dilinmaoxxxx  https://drive.google.com/drive/u/1/my-drive @wenjie  wo shi  wenjie1@gmail.com #wenjie"
    //testTwitter = "Woo! Go STRIPES!!! https:\\\/\\\/t.co\\\/vIWT26Bs2s"
    //println(getTwitterScore(testTwitter))
    // store the AFINN-111 to a map


    val sparkConf = new SparkConf().setMaster("local").setAppName("DilinTest")
    val sc = new SparkContext(sparkConf)
    val input = sc.textFile(args(0))
    // new RDD only include text
    val textRDD = input.map(parseTwitters)
    //textRDD.saveAsTextFile("mytwitterTWO")

    val rstRDD = textRDD.map(getTwitterScore).zipWithIndex().map{case (v,k) => (k+1,v)}
    // if use one Reducer may not has this  problem
    val rst = rstRDD.reduceByKey{case (x, y) => x + y}
    val sortRst= rst.sortByKey()
    //m.map((t: (String, Int)) => (t._1 + "!", "x" * t._2)

    //val rstRDD = textRDD.map(getTwitterScore)
   // val newTextRDD = textRDD.collect().foreach(splitText)

    // it should be implement by json
    sortRst.saveAsTextFile("Mao_Dilin_tweets_sentiment_first20")
  }
}

