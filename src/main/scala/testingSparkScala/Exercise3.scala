package testingSparkScala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DateType, DoubleType, LongType, StringType, StructField, StructType}


object Exercise3 {

  // Todo:  (1) From googleplaystore.csv create a Dataframe (df_2) with the structure from the table below

  //       App should be a unique value;

  //       In case of App duplicates, the column "Categories" of the resulting row should contain an array
  //       with all the possible categories (without duplicates) for that app (compare example 1 with 3);

  //       In case of App duplicates (for all columns except categories), the remaining columns should have
  //       the same values as the ones on the row with the maximum number of reviews (compare example 1 with 3).

  // App, Categories, Rating, Reviews, Size, Installs, Type, Price, Content_Rating, Genres, Last_Updated, Current_Version,
  // Minimum_Android_Version

  def main(args: Array[String]): Unit = {

    val numRowsToDisplay = 100

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Application")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // Need for filter columns
    import spark.implicits._

    val initialSchema = StructType(Array(
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
      .schema(initialSchema)
      .csv("src/main/resources/googleplaystore.csv")


    val df1 = initial_dataframe.dropDuplicates("App")
      .groupBy("App")
      .agg(collect_set("Category") as "Categories")

    val df_2 = initial_dataframe.join(df1, "App").drop("Category")
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")
      .withColumn("Genres", split(col("Genres"), ";"))
      .withColumn("Size", regexp_extract($"Size", "^[0-9.]*",0).cast(DoubleType))
      .withColumn("Price", regexp_extract($"Price", "^[0-9.]*",0).cast(DoubleType))

    df_2.show(100)
    println("Finished the display of " + numRowsToDisplay + " rows.")


    spark.stop()
  }

}
