package testingSparkScala

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_set, desc, isnan, regexp_extract, split}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object MainApp {

  //Todo: (1) From googleplaystore_user_reviews.csv create a Dataframe (df_1) with the following structure:
  //                App (String)
  //                AvgSentimentPolarity (Double) DefValue=0.0        Note: Grouped by App name

  def main(args: Array[String]) {

    val numRowsToDisplay = 100
    val defaultEx = "1"
    var ex = defaultEx
    if(args(0) != null){
      ex = args(0)
    }


    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Application")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    ex match {
      case "1" => point1(spark, numRowsToDisplay).show()
      case "2" => point2(spark, numRowsToDisplay).show()
      case "3" => point3(spark, numRowsToDisplay).show()
      case "4" => point4(spark, numRowsToDisplay).show()
      case "5" => point5(spark, numRowsToDisplay).show()
      case "default" => print("No arguments specified. Use arguments 1, 2, 3, 4 or 5 to run the app properly.")
    }


    spark.stop()
  }

  def point1(spark:SparkSession, rowsToDisplay:Int): DataFrame ={
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

    //df_1.show(rowsToDisplay)
    return df_1
  }

  def point2(spark: SparkSession, rowsToDisplay:Int): DataFrame ={
    import spark.implicits._

    val main_dataframe = spark.read
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .csv("src/main/resources/googleplaystore.csv")

    val bestApps = main_dataframe.filter($"Rating" >= 4.0).orderBy(desc("Rating"))
    val bestAppsWithoutNaNs = bestApps.where(!isnan($"Rating"))

    bestAppsWithoutNaNs.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", true)
      .option("delimiter", "§")
      .save("src/main/output/best_apps.csv")

    println()
    println("Exercício 2:")
    println()

    //bestAppsWithoutNaNs.show(rowsToDisplay)
    return bestAppsWithoutNaNs
  }

  def point3(spark: SparkSession, rowsToDisplay:Int): DataFrame ={
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

    val columns: Array[String] = df_2.columns
    val reorderedColumns: Array[String] = Array("App", "Categories", "Rating", "Reviews", "Size", "Installs",
      "Type", "Price", "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version")
    val df_2_final = df_2.select(reorderedColumns.head, reorderedColumns.tail: _*)

    println()
    println("Exercício 3: df_2")
    println()

    //df_2_final.show(rowsToDisplay)
    return df_2
  }



  def point4(spark: SparkSession, rowsToDisplay:Int): DataFrame = {
    import spark.implicits._

    val df_1 = point1(spark, rowsToDisplay)
    val df_2 = point3(spark, rowsToDisplay)

    val df_ex4 = df_2.join(df_1, df_2.col("App") === df_1.col("App")).drop(df_1.col("App"))

    val columns: Array[String] = df_ex4.columns
    val reorderedColumns: Array[String] = Array("App", "Categories", "Rating", "Reviews", "Size", "Installs",
      "Type", "Price", "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version", "Average_Sentiment_Polarity")
    val df_4_final = df_ex4.select(reorderedColumns.head, reorderedColumns.tail: _*)

    df_4_final.write
      .mode("overwrite")
      .parquet("src/main/output/googleplaystore_cleaned")

    println()
    println("Exercício 4: df_3")
    println()

    //df_4_final.show(rowsToDisplay)
    return df_4_final

  }




  def point5(spark: SparkSession, rowsToDisplay:Int): DataFrame ={
    import spark.implicits._

    val ex1 = point1(spark, rowsToDisplay)

    val ex3 = point3(spark, rowsToDisplay)

    val df_ex4 = ex3.join(ex1, ex3.col("App") === ex1.col("App")).drop(ex1.col("App"))

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

    println("Table with Average_Rating")
    df_app_rating.show(10)

    //df_genre_polarity_rating.show(rowsToDisplay)
    return df_genre_polarity_rating
  }
}
