package testingSparkScala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_set, regexp_extract, split}
import org.apache.spark.sql.types._

object Exercise4 {
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

    //----------------- Exercise 3 -----------------

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
      .option("dateFormat", "MMMMdd,yyyy")
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

    //----------------- Join ex1 & ex3 and save as Parquet file -----------------

    val df_ex4 = df_2.join(df_1, df_2.col("App") === df_1.col("App")).drop(df_1.col("App"))

    val columns: Array[String] = df_ex4.columns
    val reorderedColumns: Array[String] = Array("App", "Categories", "Rating", "Reviews", "Size", "Installs",
      "Type", "Price", "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version")
    val df_4_final = df_ex4.select(reorderedColumns.head, reorderedColumns.tail: _*)

    df_4_final.write
      .mode("overwrite")
      .parquet("src/main/output/googleplaystore_cleaned")

    println()
    println("Exercício 4: df_3")
    println()

    df_4_final.show(numRowsToDisplay)
    spark.stop()
  }
}
