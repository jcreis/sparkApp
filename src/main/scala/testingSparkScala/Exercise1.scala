package testingSparkScala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Exercise1 {
  def main(args: Array[String]) {

    val numRowsToDisplay = 10000

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Application")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val initialSchema = StructType(Array(
      StructField("App", StringType),
      StructField("Translated_Review", StringType),
      StructField("Sentiment", StringType),
      StructField("Sentiment_Polarity", DoubleType),
      StructField("Sentiment_Subjectivity", DoubleType)
    ))


    val main_dataframe = spark.read
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .schema(initialSchema)
      .csv("src/main/resources/googleplaystore_user_reviews.csv")
      .toDF("App","Translated_Review", "Sentiment", "Sentiment_Polarity", "Sentiment_Subjectivity")

    //GroupBy does the avg of double values on its own
    val test_df = main_dataframe.orderBy("App").groupBy("App").mean()

    println()
    println("Exercicio 1: df_1")
    println()

    val df_1 = test_df.select("App", "avg(Sentiment_Polarity)").show(numRowsToDisplay)

    println("Finished the display of " + numRowsToDisplay + " rows.")


    spark.stop()
  }
}
