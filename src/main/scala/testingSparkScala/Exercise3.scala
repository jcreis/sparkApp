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

    /*val finalSchema = StructType(Array(
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
    ))*/

    val initial_dataframe = spark.read
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .option("dateFormat", "MMMMdd,yyyy") // sets date to dateFormat
      .schema(initialSchema)
      .csv("src/main/resources/googleplaystore.csv")

    //initial_dataframe.printSchema()
    //initial_dataframe.show()

    val df_1 = initial_dataframe.dropDuplicates("App")
      .groupBy("App")
      .agg(collect_set("Category") as "Categories")
    //df_1.show()            initial_dataframe("App") === df_1("App")

    val combined_init_1 = initial_dataframe.join(df_1, "App").drop("Category")
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")
      .withColumn("Genres", split(col("Genres"), ";"))
      .withColumn("Size", regexp_extract($"Size", "^[0-9.]*",0).cast(DoubleType))
      .withColumn("Price", regexp_extract($"Price", "^[0-9.]*",0).cast(DoubleType))



   // val combined_with_price = combined_init_1.withColumn("Price", regexp_extract($"Price", "^[0-9.]*",0).cast(DoubleType))

    //val a = initial_dataframe.join(combined_init_1, $"Size".cast(DoubleType))

    combined_init_1.printSchema()
    combined_init_1.show()


    spark.stop()
  }

}
