package testingSparkScala

import org.apache.spark.sql.SparkSession
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

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Application")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // Need for filter columns
    import spark.implicits._

    val customSchema = StructType(Array(
      StructField("App", StringType),
      StructField("Categories", ArrayType(StringType)),
      StructField("Rating", DoubleType),
      StructField("Reviews", LongType),
      StructField("Size", DoubleType),
      StructField("Installs", StringType),
      StructField("Type", StringType),
      StructField("Price", DoubleType),
      StructField("Content_Rating", StringType),
      StructField("Genres", ArrayType(StringType)),
      StructField("Last_Updated", DateType),
      StructField("Current_Version", StringType),
      StructField("Minimum_Android_Version", StringType),


    ))

    val main_dataframe = spark.read
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .csv("src/main/resources/googleplaystore.csv")


  }

}
