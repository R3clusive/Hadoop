package com.labs1904.spark
import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes


/**
 * Spark Structured Streaming app
 *
 */
case class re(marketplace: String, customer_ID: String, review_id: String, product_id: String, product_parent: String, product_title: String,
              product_category  : String,
              star_rating      : String,
              helpful_votes     : String,
              total_votes  : String  ,
              vine   : String,
              verified_purchase  : String,
              review_headline   : String,
              review_body : String,
              review_date: String)
case class re2(marketplace: String, customer_ID: String, review_id: String, product_id: String, product_parent: String, product_title: String,
              product_category  : String,
              star_rating      : String,
              helpful_votes     : String,
              total_votes  : String  ,
              vine   : String,
              verified_purchase  : String,
              review_headline   : String,
              review_body : String,
              review_date: String, email: String)


object StreamingPipeline {
  implicit def stringToBytes(str: String): Array[Byte] = Bytes.toBytes(str)


  System.setProperty("hadoop.home.dir", "D:\\HadoopBin\\winutils-master\\hadoop-3.0.0")

  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "StreamingPipeline"
//val HADOOP_HOME = "D:\\HadoopBin\\winutils-master\\hadoop-3.0.0\\bin"
  def main(args: Array[String]): Unit = {
    try {

      val spark = SparkSession.builder()
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .config("spark.hadoop.fs.defaultFS", "hdfs://manager.hourswith.expert:8020")
        .appName(jobName).master("local[*]").getOrCreate()

      // TODO: change bootstrap servers to your kafka brokers
      val bootstrapServers = "35.239.241.212:9092,35.239.230.132:9092,34.69.66.216:9092"

      import spark.implicits._



      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "20")

        .load()
        .selectExpr("CAST(value AS STRING)").as[String]

      val splitRdd = df.map(row =>{
     val arr = row.split(',')
        re( arr(0), arr(1),arr(2), arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12),arr(13),arr(14))

   })


      val customers = splitRdd.mapPartitions(partition => {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum","cdh01.hourswith.expert:2181,cdh02.hourswith.expert:2181,cdh03.hourswith.expert:2181")
        val connection = ConnectionFactory.createConnection(conf)

        val table2 = connection.getTable(TableName.valueOf( "shared:users" ))

 val iter = partition.map(customid => {

   val get = new Get(Bytes.toBytes(customid.customer_ID)).addFamily("f1")
   val result = table2.get(get)


  val email2 =  Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail")))
   val Cust =
     re2(marketplace = customid.marketplace,
       customer_ID = customid.customer_ID,
       review_id = customid.review_id,
       product_id = customid.product_id,
       product_parent = customid.product_parent,
       product_title = customid.product_title,
       product_category = customid.product_category,
       star_rating = customid.star_rating,
       helpful_votes = customid.helpful_votes,
       total_votes = customid.helpful_votes,
       vine = customid.vine,
       verified_purchase = customid.verified_purchase,
       review_headline = customid.review_headline,
       review_body = customid.review_body,
       review_date = customid.review_date,
       email = email2)

Cust
      }).toList


        connection.close()
iter.iterator
      }
      )


customers.printSchema()
      val query = customers.writeStream
        .outputMode(OutputMode.Append())
        .format("csv")
        .option("path", "hdfs://manager.hourswith.expert:8020/user/snewberry/reviews_streaming3")
        .option("checkpointLocation", "hdfs://manager.hourswith.expert:8020/user/snewberry/reviews_checkpoint")
        .trigger(Trigger.ProcessingTime("10 seconds"))
       .start()

      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }
}
