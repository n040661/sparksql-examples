package com.sankar

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object KafkaSimpleConsumer extends App {
  val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Minutes(1))
  //SQLContext.getOrCreate(ssc.sparkContext)
  val sqlContext = new SQLContext(ssc.sparkContext)
  
  ssc.checkpoint("checkpoint")

  val topicsSet = "test".split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, topicsSet).map(_._2).window(Minutes(3), Minutes(2))

  messages.foreachRDD(rdd => {
    println("INSIDE FOREACH RDD")
    println(rdd.count)
    val sc = rdd.sparkContext
//    sc.parallelize(Seq("some string")).saveAsTextFile(path)
  })

  // Start the computation
  ssc.start()
  ssc.awaitTermination()
}