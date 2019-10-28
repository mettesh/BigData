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


    // Plukker ut id-attributten og nd-barna til wayen. Den vil også plukke ut alle taggene, med innhold, til nåværende way
    // Ved å benytte explode vil innholdet i taggene også kunne plukkes ut
    val query = wayData.select($"_id".as("Highway id"), $"nd", explode($"tag").as("Tags"))

    // Filtrerer disse på de wayene som har en tag hvor k= highway (Altså som er en highway)
    val highWayWays = query.filter($"Tags._k" === "highway")

    // Legger til en kolonne som skal inneholder antall noder for hver highway
    val highwayWithNodes = highWayWays.withColumn("Nodes count", size($"nd"))

    // Sorterer de til stigende rekkefølge
    val highwaysWithMostNodes = highwayWithNodes.orderBy($"Nodes count".desc)

    // Plukker ut det jeg vil vies
    val result = highwaysWithMostNodes.select("Highway id", "Nodes count")

    // Viser kun de 20 første. Da disse er de med flest antall noder
    result.show(20)

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

