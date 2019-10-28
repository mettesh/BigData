import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Five_AverageNumOfNodesBuilding {

  // What is the average number of nodes used to form the building ways in the extract? 

  def main(args: Array[String]) {


    val spark: SparkSession = initializeSpark(args)
    val wayData : DataFrame = searchForNodeAndTag(spark)

    import spark.implicits._

    // Plukker ut nd-barna til wayen. Den vil også plukke ut alle taggene, med innhold, til nåværende way
    val query = wayData.select( $"nd", explode($"tag").as("Tag"))

    // Filtrerer disse på de wayene som har en tag hvor k= building (Altså som er en highway)
    val buildingWays = query.filter($"Tag._k" === "building")

    val avgNodes = buildingWays.agg(avg($"nd"))
    // val numbersOfWays = sum(buildingWays)
    // Må få ut antall wayer!!
    // Legger til en kolonne som skal inneholder antall noder for hver highway
    val buildingWithNodes = buildingWays.withColumn("Nodes count", avg($"nd"))

    buildingWithNodes.show()

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

