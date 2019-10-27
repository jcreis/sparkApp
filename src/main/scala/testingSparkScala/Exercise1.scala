package testingSparkScala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Exercise1 {

  //Todo: (1) From googleplaystore_user_reviews.csv create a Dataframe (df_1) with the following structure:
  //                App (String)
  //                AvgSentimentPolarity (Double) DefValue=0.0        Note: Grouped by App name

  def main(args: Array[String]) {

    val numRowsToDisplay = 100

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



    //GroupBy does the avg of double values on its own
    val test_df = main_dataframe.orderBy("App").groupBy("App").mean()


    println()
    println("Exercício 1: df_1")
    println()

    val df_1 = test_df.select("App", "avg(Sentiment_Polarity)")
      .withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity")

    df_1.show(numRowsToDisplay)


    spark.stop()
  }
}
