package org.json.tweets

import com.mongodb.spark.MongoSpark
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.functions.{col, to_timestamp, udf}
import com.mongodb.MongoClient
import com.mongodb.client.model
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession



import java.text.SimpleDateFormat



object TweetsManipulator {
  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongodbSparkConnector")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/Tweets.tweets")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/Tweets.tweets")
      .getOrCreate()


    /*
    1.read the json-formatted tweets in the attached file and use
    Mongodb Spark library to insert them into mongodb database in
    a collection called 'tweets'
     */
    val sc = sparkSession.sparkContext

    val dataDF = sparkSession
      .read
      .format("json")
      .load("data/boulderFloodGeolocatedTweets.json")

    val confWrite = WriteConfig(
      Map("collection" -> "tweets" , "writeConcern.w" -> "majority"),
      Some(WriteConfig(sc))
    )

    /*
    2.The timestamp associated with each tweet is to be stored as a
    Date object, where the timestamp field is to be indexed.
     */

    val StringToDate = udf((created_at: String) => {
      val simpleDateFormat = new SimpleDateFormat ("E MMM dd HH:mm:ss Z yyyy")
      val date = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss")
      val ConvertToDate = date.format((simpleDateFormat.parse(created_at)))
      ConvertToDate
    })

    val dateFormat = StringToDate(col("created_at"))
    var dataset = dataDF.withColumn("created_at", dateFormat)
    val timestamp = to_timestamp(col("created_at"), "MM-dd-yyyy HH:mm:ss")
    dataset = dataset.withColumn("created_at", timestamp)
    MongoSpark.save(dataset, confWrite)






  }
}
