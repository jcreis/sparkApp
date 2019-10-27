package testingSparkScala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Exercise5 {
  def main(args: Array[String]) {

    val numRowsToDisplay = 100

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Application")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    //----------------- Exercise 1 -----------------

    val ex1Schema = StructType(Array(
      StructField("App", StringType),
      StructField("Translated_Review", StringType),
      StructField("Sentiment", StringType),
      StructField("Sentiment_Polarity", DoubleType),
      StructField("Sentiment_Subjectivity", DoubleType)
    ))

    val main_dataframe = spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .schema(ex1Schema)
      .csv("src/main/resources/googleplaystore_user_reviews.csv")

    val test_df = main_dataframe.orderBy("App").groupBy("App").mean()
    val df_1 = test_df.select("App", "avg(Sentiment_Polarity)")
      .withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity")

    val ex3Schema = StructType(Array(
      StructField("App", StringType),
      StructField("Category", StringType),
      StructField("Rating", DoubleType),
      StructField("Reviews", LongType),
      StructField("Size", StringType),
      StructField("Installs", StringType),
      StructField("Type", StringType),
      StructField("Price", StringType),
      StructField("Content Rating", StringType),
      StructField("Genres", StringType),
      StructField("Last Updated", DateType),
      StructField("Current Ver", StringType),
      StructField("Android Ver", StringType),
    ))

    val initial_dataframe = spark.read
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .option("dateFormat", "MMMMdd,yyyy") // sets date to dateFormat
      .schema(ex3Schema)
      .csv("src/main/resources/googleplaystore.csv")

    val df = initial_dataframe.dropDuplicates("App")
      .groupBy("App")
      .agg(collect_set("Category") as "Categories")

    val df_2 = initial_dataframe.join(df, "App").drop("Category")
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")
      .withColumn("Genres", split(col("Genres"), ";"))
      .withColumn("Size", regexp_extract($"Size", "^[0-9.]*",0).cast(DoubleType))
      .withColumn("Price", regexp_extract($"Price", "^[0-9.]*",0).cast(DoubleType))

    val df_ex4 = df_2.join(df_1, df_2.col("App") === df_1.col("App")).drop(df_1.col("App"))

    // avg(Rating)
    val df_app_rating = df_ex4.groupBy("App").avg("Rating")
      .withColumnRenamed("avg(Rating)", "Average_Rating")


    val df_app_genre_polarity = df_ex4.select("App", "Genres", "Average_Sentiment_Polarity")

    val df_genre_polarity_rating = df_app_genre_polarity
      .join(df_app_rating, df_app_genre_polarity.col("App") === df_app_rating.col("App"))
      .drop("App").groupBy("Genres").count()
      .withColumnRenamed("Genres", "Genre")
      .withColumnRenamed("count", "Count")


    /*val a = df_ex4
     .groupBy("App", "Genres", "Average_Sentiment_Polarity")
     .avg("Rating")
     .withColumnRenamed("Genres", "Genre")
     .withColumnRenamed("avg(Rating)", "Average_Rating")
    val b = a
      .drop("App")
      //.groupBy("Genre", "Average_Sentiment_Polarity", "Average_Rating").count()
      .groupBy("Genre").count()
      .withColumnRenamed("count", "Count")
        .join(a, a.col("Average_Rating")).show()*/

    df_genre_polarity_rating.write
      .mode("overwrite")
      .parquet("src/main/output/googleplaystore_metrics")

    println()
    println("Exercício 5:")
    println()

    println("Table with Average_Rating (couldn't join with df_4 without conflicts)")
    df_app_rating.show(10)


    println("df_4")
    df_genre_polarity_rating.show(numRowsToDisplay)

    spark.stop()
  }
}
