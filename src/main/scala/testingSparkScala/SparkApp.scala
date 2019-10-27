package testingSparkScala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType}

// Example class
object SparkApp {

    def main(args: Array[String]) {

      val spark = SparkSession.builder
        .master("local[*]")
        .appName("Spark Application")
        .getOrCreate()
      import spark.implicits._
      spark.sparkContext.setLogLevel("WARN")

      val customSchema = StructType(Array(
        StructField("name", StringType),
        StructField("class", StringType),
        StructField("dorm", StringType),
        StructField("room", StringType),
        StructField("gpa", DoubleType),
      ))

      val test_dataframe = spark.read
        .option("header","true")
        .schema(customSchema)
        .csv("src/main/resources/test.csv")

      //test_dataframe.show()
      //test_dataframe.schema.printTreeString()

      val studentsGPA = test_dataframe.filter($"gpa" > 3.5)
      studentsGPA.show()
      val studentsname = test_dataframe.filter('name === "Sandy Allen")
      studentsname.show()
      val combinedTables = studentsGPA
        .join(studentsname, studentsGPA("name") === studentsname("name"))
      combinedTables.show()



      /*
          val test_df = main_dataframe.groupBy($"App")
      */


      //df_1.show(100)
      //main_dataframe.show(100)
      //println("----------------------------------------------------------------------------------")

      //val sentimentPolarity = main_dataframe.filter($"Sentiment_Polarity" > 0.5)


      //sentimentPolarity.show(100)

      //println("----------------------------------------------------------------------------------")

      //val appName = main_dataframe.filter($"App" === "10 Best Foods for You").drop("App")

      //appName.show(100)

      //println("----------------------------------------------------------------------------------")


      //val combinedTables = appName.join(sentimentPolarity, Seq("App", "Sentiment_Polarity"))
      //combinedTables.show()




      spark.stop()
    }


}
