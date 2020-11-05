//reference: https://github.com/caroljmcdonald/mapr-sparkml-sentiment-classification/blob/master/src/main/scala/stream/StructuredStreamingConsumer.scala

package sparkstreaming

import java.util.HashMap
import java.util.{Date, Properties}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

import java.nio.ByteBuffer
import scala.math.BigInt

import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.{Pipeline, PipelineModel}
//import org.apache.spark.ml.tuning._

import org.json4s._, org.json4s.native.JsonMethods._
import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD

object StreamingConsumer {
   
	val spark = SparkSession.builder.master("local[2]").appName("PredictAndStore").getOrCreate()
	import spark.implicits._
		
	val datetime = DateTime.now()
	val stationsDf = spark.read
    	                  .option("header", "true")
						  .option("inferSchema", "true")
    	                  .csv("/home/florent/id2221_project/data/outputs/stations.csv")
						  .cache()

	val model = PipelineModel.load("/home/florent/id2221_project/training/models/DTmodel")
	val schema = new StructType()
					.add(StructField("id", IntegerType, true))
					.add(StructField("year", IntegerType, true))
  					.add(StructField("month", IntegerType, true))
  					.add(StructField("day", IntegerType, true))
  					.add(StructField("hour", IntegerType, true))
  					.add(StructField("min", IntegerType, true))
  					.add(StructField("nb_bikes_available", IntegerType, true))

	case class output_row (
		id: Int,
		year: Int,
		month: Int,
		day: Int,
		hour: Int,
		min: Int,
		nb_bikes_available: Int
	)

	def main(args: Array[String]): Unit = {

		// CASSANDRA connection
		val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
		val session = cluster.connect()
		session.execute("CREATE KEYSPACE IF NOT EXISTS bike_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
		session.execute("CREATE TABLE IF NOT EXISTS bike_space.bike_available (station_name text, date text, time text, predicted_nb_bikes_available int, PRIMARY KEY(station_name, date, time));")
		
		// SPARK STREAMING
		val sc = spark.sparkContext
		val ssc = new StreamingContext(sc, Seconds(5))
		ssc.sparkContext.setLogLevel("ERROR")
		//ssc.checkpoint("/tmp/chk")

		// KAFKA connection
		val kafkaConf = Map(
                         "metadata.broker.list" -> "localhost:9092",
                         "zookeeper.connect" -> "localhost:2181",
                         "group.id" -> "kafka-spark-streaming",
                         "zookeeper.connection.timeout.ms" -> "1000"
                         )
		val topics = Set("bikes")
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)
		
		
		val pairs = messages.map(message => message._2.split(",")).map(rawPair => (rawPair(0).toInt, rawPair(1).toInt))

		// STREAMING TOPICS
		pairs.foreachRDD(rdd => 
			if (!rdd.isEmpty()) {
				val data = rdd.map(pair => Row(pair._1, datetime.getYear, datetime.getMonthOfYear, datetime.getDayOfMonth, datetime.getHourOfDay, datetime.getMinuteOfHour, pair._2))
				var df = spark.createDataFrame(data, schema)
				df = df.join(
						stationsDf.select("id", "name", "latitude", "longitude"), 
                  		Seq("id"),
                  		"left")
					   .withColumnRenamed("nb_bikes_available", "label")
				df = df.na.fill("NA", Seq("name"))
            		   .na.fill(0.0, Seq("latitude"))
					   .na.fill(0.0, Seq("longitude"))
				val prediction = model.transform(df)
				var finalDF = prediction
							  .withColumnRenamed("name", "station_name")
							  .withColumn("date", concat_ws("-",$"year",$"month",$"day"))
							  .withColumn("time", concat_ws(":",$"hour",$"min"))
							  .withColumn("prediction", prediction("prediction").cast(IntegerType))
							  .withColumnRenamed("prediction", "predicted_nb_bikes_available")
							  .select("station_name", "date", "time", "predicted_nb_bikes_available")
				val finalRDD: RDD[(String, String, String, Int)] = finalDF.as[(String, String, String, Int)].rdd
				finalRDD.collect().foreach(println)
				finalRDD.saveToCassandra("bike_space0", "bike_available0", SomeColumns("station_name", "date", "time", "predicted_nb_bikes_available"))
			}
		)
		
		ssc.start()
		ssc.awaitTermination()
    }
}
