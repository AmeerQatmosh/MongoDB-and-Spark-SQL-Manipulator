package org.json.tweets

import com.mongodb.spark.MongoSpark
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.functions.{col, to_timestamp, udf}
import com.mongodb.MongoClient
import org.mongodb.scala.model.{Filters, Indexes}
import java.text.SimpleDateFormat
import Helpers._
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

    /*
    3.the geo-coordinates of tweets should be indexed properly to ensure
    a fast spatial-based retrieval
     */
    val mongoClient = new MongoClient("localhost", 27017)
    val Tweets = mongoClient.getDatabase("Tweets")
    val tweetsCollection = Tweets.getCollection("tweets")
    tweetsCollection.createIndex(Indexes.geo2dsphere("coordinates.coordinates"))
    //tweetsCollection.createIndex(Indexes.geo2dsphere("coordinates.coordinates")).printResults()
    tweetsCollection.createIndex(Indexes.ascending("created_at"))

    /*
    4.calculate the number of occurrences of word w published within
    a circular region of raduis (r), having a central point of (lon, lat),
    mentioned in tweets published during the time interval (start, end).
    Perform this operation by two ways:
        a. using MongoSpark, by collecting tweets and filtering
        them spatio-temporally using dataframe apis.
        b. using mongodb library by sending a normal mongoDB query
        to filter by time and space.
        c. Text indexing is optional
     5.Run the application as follows:
        WordFreqCalculator.scala w r lon lat start end
     */

  }
}


