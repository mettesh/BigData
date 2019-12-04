// Which 15 highways contains the most number of traffic calming=hump?

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Seven_15HighwaysMostTrafficCalmingHump {

  def main(args: Array[String]) {

    var systemTime = System.currentTimeMillis()

    val spark: SparkSession = initializeSpark(args)
    val wayData : DataFrame = searchForWayAndTag(spark)
    val nodeData : DataFrame = searchForNodeAndTag(spark)
    import spark.implicits._

    systemTime = System.currentTimeMillis() - systemTime
    printf("Oppstartstid\t: %6.3f s\n", systemTime / 1000.0)

    var time = System.currentTimeMillis()

    /* WAYS */

    // Henter id og barnenodene til wayer
    val ways = wayData.select($"_id".as("Way id"), explode($"nd").as("WayNodes"))

    // For plukker så ut alle _ref-nummeret for hver way-id (Disse referer til blandt annet humps)
    val wayNodesAndId = ways.select("Way id", "WayNodes._ref")

    /* Nodes */

    // Henter id og barnetagger til noder
    val nodes = nodeData.select($"_id".as("Node id"), explode($"tag").as("Nodetags"))

    // Filtrere slik at man kun får de nodene som er en traffic calming=hump
    val nodesWithTrafficHump = nodes.filter($"Nodetags._k" === "traffic_calming" && $"Nodetags._v" === "hump")


    /* JOINING */
    //joining de to tabellene sammen på nodene sin id, og wayene sin _ref-id
    val joinedTables = nodesWithTrafficHump.join(wayNodesAndId, nodesWithTrafficHump("Node id") === wayNodesAndId("_ref"))

    // Gruppere de så på way-id slik at man får en opptelling for de ulike ref-ene. Sortere de synkende på antall
    val table = joinedTables.groupBy("Way id").count().orderBy($"count".desc)


    table.show(15)

    time = System.currentTimeMillis() - time
    printf("Kjøretid i sekunder\t: %6.3f s\n", time / 1000.0)

  }

  private def searchForNodeAndTag(spark : SparkSession) = {
    val nodeData = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "node")
      .load("input/oslo.osm")

    nodeData
  }

private def searchForWayAndTag(spark : SparkSession) = {
  val wayData = spark.read.format("com.databricks.spark.xml")
    .option("rootTag", "osm")
    .option("rowTag", "way")
    .load("input/oslo.osm")

  wayData
}

  private def initializeSpark(args: Array[String]) = {
    val conf = new SparkConf().setMaster(args(0)).setAppName("Task 6")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    spark
  }
}

