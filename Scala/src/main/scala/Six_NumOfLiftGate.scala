import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Six_NumOfLiftGate {

  // How many ways of types ”highway=path”, ”highway=service”, ”high- way=road”, ”highway=unclassified” contains a node with the tag ”bar- rier=lift gate”?

  //  <tag k="barrier" v="lift_gate"/>
  //  <tag k="highway" v="path"/>

  def main(args: Array[String]) {


    val spark: SparkSession = initializeSpark(args)
    val wayData : DataFrame = searchForNodeAndTag(spark)

    import spark.implicits._

    // Plukker ut nd-barna til wayen. Den vil også plukke ut alle taggene, med innhold, til nåværende way
    val query = wayData.select( $"nd", explode($"tag").as("Tag"))

    // Filtrerer disse på de wayene som har en tag hvor k= highway (Altså som er en highway-way)
    val highWayWays = query.filter($"Tag._k" === "highway")

    // Filtrer disse igjen på highwayene som har value: path, service, road eller unclassified
    val highWaysWithCorrectType = highWayWays.filter($"Tag._v" === "path" || $"Tag._v" === "service" || $"Tag._v" === "road" || $"Tag._v" === "unclassified")

    val hasBarrierLiftGate = highWaysWithCorrectType.filter($"Tag._k" === "barrier" && $"Tag._v" === "lift_gate")

    hasBarrierLiftGate.show()
    // Oppretter en ny kolonne hvor opptellingen av noder per way skal stå
    //val numbersOfNodes = buildingWays.withColumn("Nodes count", size($"nd"))

    // Tar gjennomsnittet av kolonnen med opptellinger og viser denne
    //numbersOfNodes.select(avg($"Nodes count").as("Average number of nodes")).show()
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

