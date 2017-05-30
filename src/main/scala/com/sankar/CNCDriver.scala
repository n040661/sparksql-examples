package com.scb.cnc.payments

import scala.collection.mutable.ListBuffer

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import com.scb.cnc.payments.reader.ReferenceTableReader
import com.scb.cnc.payments.reader.RemedyTableReader
import com.scb.cnc.payments.reader.UDMMainTableReader
import com.scb.cnc.payments.util.AgeingUtil
import com.scb.cnc.payments.util.DQUtil
import com.scb.cnc.payments.util.Enrichment
import com.scb.cnc.payments.util.HolidayCheck
import com.scb.cnc.payments.writer.HBaseWriter
import com.scb.cnc.payments.writer.HDFSWriter

import kafka.serializer.StringDecoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import com.scb.cnc.payments.util.IncidentsViewUtil
import com.scb.cnc.payments.util.StagingUtil
import com.scb.cnc.payments.util.PreProcessFlow
import org.apache.spark.sql.DataFrame
import java.time.LocalTime

/**
 * Driver class used to execute all the Dashboard's logic for UPV and KCM.
 * The flow starts with reads the data from Kafka Topic and populate the data in HBase table then read the
 * data from HBase table and apply transformation logic and populate the final result in Cassandra Database.
 * @author 1554161
 */
object CNCDriver {

  @transient lazy val log = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    try {
      val sparkConf = new SparkConf().setAppName("CNC Payments")
      sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")//Added for Write Ahead Logs
      val ssc = new StreamingContext(sparkConf, Seconds(40))
      val sc = ssc.sparkContext
      val sqlContext: SQLContext = new HiveContext(sc)
      
//      val checkPointLocation = sc.getConf.get("spark.check.point.location")////Added for Write Ahead Logs
//      ssc.checkpoint(checkPointLocation)////Added for Write Ahead Logs

      val topicsList = sc.getConf.get("spark.topics.list")
      val groupId = sc.getConf.get("spark.group.id")
      val sasl = sc.getConf.get("spark.sasl")
      val brokerList = sc.getConf.get("spark.metadata.broker.list")
      val zookeeperURL = sc.getConf.get("spark.zookeeper.connect")
      val refFilePath = sc.getConf.get("spark.ref.table.file.path")
      val remedyTopic = sc.getConf.get("spark.remedy.topic.name")
      val sourceHDFSPath = sc.getConf.get("spark.source.hdfs.path")

      val topicMap = topicsList.split(",").map { x => (x, 3) }.toMap
      val remedyTopicMap = remedyTopic.split(",").map { x => (x, 3) }.toMap

      var dStreamRDDList = new ListBuffer[RDD[String]]
      var remedyStreamRDDList = new ListBuffer[RDD[String]]
      val kafkaParams = Map("group.id" -> groupId,
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> sasl,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        "metadata.broker.list" -> brokerList,
        "zookeeper.connect" -> zookeeperURL)

      val dStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
      //val remedyStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, remedyTopicMap, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
      dStream.foreachRDD(rdd =>
        {
          if (rdd.count() > 0) {
            log.info("INSIDE STORING RDD TO DSTERAM LIST and THE RDD COUNT IS:" + rdd.count())
            dStreamRDDList += rdd
            log.info("THE DSTREAM RDD LIST SIZE IS:" + dStreamRDDList.size)
          } else {
            log.info("IN ELSE LOOP and the RDD COUNT IS:" + rdd.count)
            if (dStreamRDDList.size > 0) {
              log.info("DStream RDD List contains all the topic's data")
              val hbaseRDD = ssc.sparkContext.union(dStreamRDDList).cache()
              log.info("Number of records in the joined RDD:" + hbaseRDD.count())
              if (hbaseRDD.count() > 0) {
                log.info("Storing Raw data to HDFS Started")
                HDFSWriter.storeRawDataToHDFS(hbaseRDD, sqlContext, sourceHDFSPath)
                log.info("Storing Raw data to HDFS Completed")

                log.info("Filtering Invalid / poison records Started")
                HDFSWriter.writeInvalidRecords(hbaseRDD, sourceHDFSPath)
                log.info("Filtering Invalid / poison records Completed")

                log.info("Fetch Records with 125 Columns Started")
                val recordsWith125Cols = DQUtil.getRowsWith125Cols(hbaseRDD).cache()
                log.info("Number of Records with 125 Columns:" + recordsWith125Cols.count())
                log.info("Fetch Records with 125 Columns  Completed")

                val rejectRecords = DQUtil.getRejectRecords(recordsWith125Cols)
                log.info("Reject Records Count:" + rejectRecords.count())
                HDFSWriter.writeRejectRecords(rejectRecords, sourceHDFSPath, sqlContext)
                val validRecords = DQUtil.getValidRecords(recordsWith125Cols)
                log.info("Valid Records Count:" + validRecords.count())

                //Staging table load
                val stagingDF = StagingUtil.saveRDDToStagingTable(validRecords, sqlContext, refFilePath)
                dStreamRDDList.clear
                //Pre Process Flow
                val preProcessedDF = PreProcessFlow.preProcessLogic(stagingDF, sqlContext)
                //Enrichment logic
                val udmEnrichmentDF = Enrichment.executeEnrichmentLogic(preProcessedDF, sqlContext, refFilePath)
                log.info("Clearing the DStream RDD List")

                HBaseWriter.writeDFToHbaseMainTable(udmEnrichmentDF)
                val historyTableDF = Enrichment.executeHistoryLogic(udmEnrichmentDF)
                HBaseWriter.writeDFToHbaseHistoryTable(historyTableDF)
                //UPV and KCM Logic
                executeUPVAndKCM(sqlContext, refFilePath)
              }
            }
          }
        })
      /*remedyStream.foreachRDD(rdd =>
        {
          if (rdd.count() > 0) {
            log.info("INSIDE STORING RDD TO REMEDY DSTERAM LIST and THE RDD COUNT IS:" + rdd.count())
            remedyStreamRDDList += rdd
            log.info("THE REMEDY DSTREAM RDD LIST SIZE IS:" + remedyStreamRDDList.size)
          } else {
            if (remedyStreamRDDList.size > 0) {
              log.info("Remedy DStream RDD List contains all the topic's data")
              val remedyRDD = ssc.sparkContext.union(remedyStreamRDDList)
              log.info("Number of records in the REMEDY RDD:" + remedyRDD.count())
              if (remedyRDD.count() > 0) {

                val remedyRowRDD = remedyRDD.map(row => {
                  rowToColumnMapping(row)
                })
                val remedyStructType = HBaseWriter.getRemedyStructType()
                val initRemedyDF = sqlContext.createDataFrame(remedyRowRDD, remedyStructType)
                val remedyDF = initRemedyDF.withColumn("remedyKey", col("INCIDENT_NUMBER"))
                remedyStreamRDDList.clear()
                log.info("Remedy DF Count:"+remedyDF.count())
                val remedySchema = HBaseWriter.getRemedyTableSchema()
                remedyDF.write.options(Map(HBaseTableCatalog.tableCatalog -> remedySchema))
                  .format("org.apache.spark.sql.execution.datasources.hbase")
                  .save()
              }
            }
          }
        })*/
      ssc.start()
      ssc.awaitTermination()
    } catch {
      case e: Throwable => log.error(e.getMessage)
    }
  }

  /**
   * This method used to execute UPV and KCM flow.
   * @param sqlContext
   * @param refFilePath
   */
  def executeUPVAndKCM(sqlContext: SQLContext, refFilePath: String) {

    try {
      val udmDF = UDMMainTableReader.getUDMMainTableDF(sqlContext)
      val holidayTableDF = ReferenceTableReader.getHolidayTableDF(sqlContext, refFilePath)
      val holidayCountryMappingDF = ReferenceTableReader.getHolidayCountryMappingTableDF(sqlContext, refFilePath)
      val countryHolidaysMap = HolidayCheck.getCountryHolidaysMap(holidayTableDF, holidayCountryMappingDF)
      val workingDaysMap = HolidayCheck.getWorkingDaysMap(sqlContext, refFilePath)
      val ageingDF = AgeingUtil.getAgeingColumn(udmDF, sqlContext, countryHolidaysMap, workingDaysMap)
      val udmMainDF = AgeingUtil.getTCTColumn(ageingDF, sqlContext, countryHolidaysMap, workingDaysMap).cache()
      writeAgeingTCTInHDFS(udmMainDF)
      val currencyCutoffDF = ReferenceTableReader.getCurrencyCutOffTableDF(sqlContext, refFilePath)
      val countryHubMappingDF = ReferenceTableReader.getCountryHubMappingTableDF(sqlContext, refFilePath)
      //val remedyDF = RemedyTableReader.getRemedyTableDF(sqlContext).cache()
      //IncidentsViewUtil.executeIncidentsView(sqlContext, remedyDF)
      UPV.executeUPVFlow(sqlContext, refFilePath, udmMainDF, currencyCutoffDF, countryHubMappingDF)
      //KCM.executeKCMFlow(sqlContext, refFilePath, udmMainDF, remedyDF, countryHubMappingDF)
    } catch {
      case e: Throwable => log.error(e.getMessage)
    }
  }
  
  def writeAgeingTCTInHDFS(udmMainDF: DataFrame) {
    val ageingDF = udmMainDF.select("txn_uid", "fn_ageing", "fn_tct")
    ageingDF.write.text("/shared/payments/test/ageing/"+LocalTime.now().getHour.toString() + LocalTime.now().getMinute.toString()+"/")
  }

  /**
   * This method used to map the row to Remedy table columns.
   */
  def rowToColumnMapping(row: String): Row = {
    val eo = row.split("\",\"", -1)
    Row(eo(0).replace("\"", ""), eo(1), eo(2), eo(3), eo(4), eo(5), eo(6), eo(7), eo(8), eo(9), eo(10),
      eo(11), eo(12), eo(13), eo(14), eo(15), eo(16), eo(17), eo(18), eo(19), eo(20),
      eo(21), eo(22).replace("\"", ""))
  }
}