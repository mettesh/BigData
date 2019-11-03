import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Seven_MostNumberofTrafficCalmingHump {

  // Which 15 highways contains the most number of traffic calming=hump

  def main(args: Array[String]) {


    val spark: SparkSession = initializeSpark(args)
    val wayData : DataFrame = searchForNodeAndTag(spark)

    import spark.implicits._

    // Plukker ut nd-barna til wayen. Den vil også plukke ut alle taggene, med innhold, til nåværende way
    val query = wayData.select( $"nd", explode($"tag").as("Tag"))

    // Filtrerer disse på de wayene som har en tag hvor k= building (Altså som er en building-way)
    val buildingWays = query.filter($"Tag._k" === "building")

    val numbersOfNodes = buildingWays.withColumn("Nodes count", size($"nd"))

    numbersOfNodes.select(avg($"Nodes count").as("Average number of nodes")).show()
  }

  private def searchForNodeAndTag(spark : SparkSession) = {
    val nodeData = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "way")
      .load("input/oslo.osm")

    nodeData
  }

  private def initializeSpark(args: Array[String]) = {
    val conf = new SparkConf().setMaster(args(0)).setAppName("Two - How many addr:street tags exist for each street?")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    spark
  }
}

