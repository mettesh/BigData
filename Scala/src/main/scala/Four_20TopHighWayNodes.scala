import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Four_20TopHighWayNodes {

  // Which 20 highways contains the most nodes?

  def main(args: Array[String]) {

    //Initaliserer spark
    val spark: SparkSession = initializeSpark(args)

    //Henter inn data basert på ønskede tagger (Way-er):
    val wayData : DataFrame = searchForNodeAndTag(spark)

    import spark.implicits._


    // Plukker ut child-tags og id-attributten til wayene
    val query = wayData.select($"nd", $"_id", explode($"tag").as("Tags"))

    // Filtrerer disse på de som er en highway
    val highWayWays = query.filter($"Tags._k" === "highway")
    val highwayWithNodes = highWayWays.withColumn("Nodes count", size($"nd"))

    val highwaysWithMostNodes = highwayWithNodes.orderBy($"Nodes count".desc)

    highwaysWithMostNodes.show(20)


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

