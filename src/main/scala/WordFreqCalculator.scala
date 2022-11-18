package org.json.tweets

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{to_timestamp, udf}
import java.text.{DateFormat, SimpleDateFormat}

import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters.near

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import org.mongodb.scala.model.geojson._
import org.mongodb.scala.model.Indexes
import org.mongodb.scala.model.Filters
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender

object WordFreqCalculator {
  def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val path = "data/boulderFloodGeolocatedTweets.json"
    val tweetsDatabase = "Tweets"
    val tweetsCollection = "tweets"
    val sparkSession = SparkSession
      .builder()
      .appName("tweets")
      .master("local[*]")
      .config("spark.mongodb.input.uri", s"mongodb://127.0.0.1/$tweetsDatabase.$tweetsCollection")
      .config("spark.mongodb.output.uri", s"mongodb://127.0.0.1/$tweetsDatabase.$tweetsCollection")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val dataDf = readDataSet(path, sparkSession)
    mongoDB(sparkSession, sc, dataDf, tweetsCollection)

    /*
    1.read the json-formatted tweets in the attached file and use
    Mongodb Spark library to insert them into mongodb database in
    a collection called 'tweets'
     */

    val mongoClient: MongoClient = new MongoClient("localhost", 27017)
    val database = mongoClient.getDatabase(tweetsDatabase)
    val collection = database.getCollection(tweetsCollection)
    createGeoCoordinatesIndexes(collection)

    val w = args(0)
    val r = args(1).toInt
    val lon = args(2).toDouble
    val lat = args(3).toDouble
    val start = args(4).toLong
    val end = args(5).toLong

    val wordFrequency: Int = SearchQuery(collection, w, r.toInt, lon, lat, start, end)
    println("*******************************************************")
    println(s"The searched word $w occurrences: $wordFrequency times")
    println("*******************************************************")
  }

  /*
  2.The timestamp associated with each tweet is to be stored as a
  Date object, where the timestamp field is to be indexed.
   */

  def stringToDate(spark: SparkSession, dataframe: DataFrame): DataFrame = {

    import spark.implicits._

    val change_format = (timeStamp: String) => {
      val formatter: DateFormat = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy")
      val formatted_date = formatter.parse(timeStamp)
      val new_date = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss ZZZZ").format(formatted_date)
      new_date
    }

    val dateUDF = udf(change_format)
    var data_df = dataframe.withColumn("created_at", dateUDF('created_at))
    data_df = data_df.withColumn("created_at", to_timestamp($"created_at", "MM/dd/yyyy HH:mm:ss Z"))
    data_df
  }

  def readDataSet(path: String, sparkSession: SparkSession): DataFrame = {

    val dataDf = sparkSession
      .read
      .format("json")
      .load(path)
    dataDf
  }

  def mongoDB(sparkSession: SparkSession, sc: SparkContext, dataframe: DataFrame, tweetsCollection: String): Unit = {
    val new_data_df = stringToDate(sparkSession, dataframe)
    val writeConfig = WriteConfig(Map("collection" -> s"$tweetsCollection",
      "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
    MongoSpark.save(new_data_df, writeConfig)
  }

  /*
  3.the geo-coordinates of tweets should be indexed properly to ensure
  a fast spatial-based retrieval
   */
  def createGeoCoordinatesIndexes(tweetsCollection: MongoCollection[org.bson.Document]): Unit = {
    tweetsCollection.createIndex(Indexes.geo2dsphere("coordinates.coordinates"))
    tweetsCollection.createIndex(Indexes.ascending("created_at"))
  }
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

  def SearchQuery(tweetsCollection: MongoCollection[org.bson.Document], w: String, r: Int, lon: Double,
                  lat: Double, start: Long, end: Long):
  Int = {
    val refPoint = Point(Position(lon, lat))
    val epoch_formatter: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    val start_date_formatted = epoch_formatter.parse(epoch_formatter.format(start))
    val end_date_formatted = epoch_formatter.parse(epoch_formatter.format(end))
    val collections_within_date_and_time = tweetsCollection.find(
      Filters.and(
        Filters.and(
          Filters.gt("created_at", start_date_formatted),
          Filters.lt("created_at", end_date_formatted)),
        near("coordinates.coordinates", refPoint, r, 0)))
      .iterator()
      .toList
    val word_count = collections_within_date_and_time
      .map(doc => doc
        .get("text")
        .toString)
      .flatMap(doc => doc
        .split("[.{},/!@#$%^&*()_+?<>\" ]"))
      .count(_.toLowerCase() == w.toLowerCase())
    word_count
  }
}
