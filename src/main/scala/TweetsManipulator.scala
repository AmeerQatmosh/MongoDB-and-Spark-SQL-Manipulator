package org.json.tweets

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession

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

    val dataset = sparkSession.read
      .json("data/boulderFloodGeolocatedTweets.json")

    dataset.write.format("com.mongodb.spark.sql.DefaultSource")
      .option("database", "Tweets")
      .option("collection", "tweets")
      .mode("append")
      .save()

  }
}
