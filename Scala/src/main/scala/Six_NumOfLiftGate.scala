import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Six_NumOfLiftGate {

  // How many ways of types ”highway=path”, ”highway=service”, ”high- way=road”, ”highway=unclassified” contains a node with the tag ”bar- rier=lift gate”?

  def main(args: Array[String]) {

    var systemTime = System.currentTimeMillis()

    val spark: SparkSession = initializeSpark(args)
    val wayData : DataFrame = searchForNodeAndTag(spark)
    import spark.implicits._

    systemTime = System.currentTimeMillis() - systemTime
    printf("Oppstartstid\t: %6.3f s\n", systemTime / 1000.0)

    var time = System.currentTimeMillis()

    // Plukker wayene med highway (Av korrekt type)
    val query = wayData.select( $"_id", explode($"tag").as("HighwayTag"))
    val highWayWays = query.filter($"HighwayTag._k" === "highway")
    val highWaysWithCorrectType = highWayWays.filter($"HighwayTag._v" === "path" || $"HighwayTag._v" === "service" || $"HighwayTag._v" === "road" || $"HighwayTag._v" === "unclassified")

    // Plukker ut wayene med lift_gate:
    val query2 = wayData.select( $"_id", explode($"tag").as("BarrierTag"))
    val barrierWays = query2.filter($"BarrierTag._k" === "barrier")
    val liftGateBarrier = barrierWays.filter($"BarrierTag._v" === "lift_gate")

    // Merger nå disse tabellene sammen på samme id
    val tableJoin = highWaysWithCorrectType.join(liftGateBarrier, highWaysWithCorrectType("_id") === liftGateBarrier("_id"))

    // Grupperer de etter highwaytype
    val groupedBy = tableJoin.groupBy($"HighwayTag._v".as("Highway type")).count()

    groupedBy.show()

    time = System.currentTimeMillis() - time
    printf("Kjøretid i sekunder\t: %6.3f s\n", time / 1000.0)

  }

  private def searchForNodeAndTag(spark : SparkSession) = {
    val nodeData = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "way")
      .load("input/oslo.osm")

    nodeData
  }

  private def initializeSpark(args: Array[String]) = {
    val conf = new SparkConf().setMaster(args(0)).setAppName("Task 6")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    spark
  }
}

