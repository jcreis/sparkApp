package testingSparkScala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Exercise2 {

  // Todo:  (1) Read googleplaystore.csv as a Dataframe
  //        (1.1) obtain all Apps with a "Rating" greater or equal to 4.0 sorted in descending order.
  //        (2) Save that Dataframe as a CSV (delimiter: "§") named "best_apps.csv"

  def main(args: Array[String]): Unit = {

    val numRowsToDisplay = 100

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Application")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // Need for filter columns
    import spark.implicits._



    val main_dataframe = spark.read
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .csv("src/main/resources/googleplaystore.csv")
    //main_dataframe.show()

    val bestApps = main_dataframe.filter($"Rating" >= 4.0).orderBy(desc("Rating"))
    val bestAppsWithoutNaNs = bestApps.where(!isnan($"Rating"))

    /*bestAppsWithoutNaNs//.select("App", "Rating")
      .coalesce(1)
      .write
      .csv("src/main/output/best_apps.csv")*/

    bestAppsWithoutNaNs.coalesce(1)
        .write
        .format("com.databricks.spark.csv")
        .mode("overwrite")
        .option("header", true)
        .option("delimiter", "§")
        .save("src/main/output/best_apps.csv")

    bestAppsWithoutNaNs.show(1000)
    println("Finished the display of " + numRowsToDisplay + " rows.")


    spark.stop()
  }
}
