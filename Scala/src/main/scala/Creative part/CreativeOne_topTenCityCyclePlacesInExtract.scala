import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CreativeOne_topTenCityCyclePlacesInExtract {

  // Hvilke 10 steder i extracten er bysykkel mest populært å starte turen fra?

  def main(args: Array[String]) {

    var time = System.currentTimeMillis()

    val spark: SparkSession = initializeSpark(args)
    val osloDataFrame : DataFrame = searchForNodeAndTag(spark)
    val cityCyclingDataFrame : DataFrame = searchForNodeAndTagCityCycling(spark)

    import spark.implicits._

    // Fra oslo.osm plukkes det ut de ulike addr:street-ene som finnes i extractet:
    val streetsInExtract = osloDataFrame.select($"_k", $"_v").filter($"_k" === "addr:street")

    //Fra bysykkel.csv plukkes det ut de ulike sykkelstasjonene ut med en opptelling på antall turer derifra
    val tripsFromStations = cityCyclingDataFrame.select("start_station_name").groupBy("start_station_name").count()

    // Kobler her sammen de to tabellene på adresse/stedsnavnene:
    val tableJoin = osloDataFrame.join(tripsFromStations, streetsInExtract("_v") === tripsFromStations("start_station_name"))

    // Henter ut det jeg ønsker etter koblingen og sorterer tabellen synkende på antall turer
    val result = tableJoin.select($"_v".as("Place in extract"), $"count".as("Numbers of trips")).orderBy($"Numbers of trips".desc).distinct()

    // Henter ut de 10 første, da jeg vil ha frem de 10 mest populære stedene
    result.show(10)

    time = System.currentTimeMillis() - time
    printf("Kjøretid i sekunder\t: %6.3f s\n", time / 1000.0)

  }

  private def initializeSpark(args: Array[String]) = {
    val conf = new SparkConf().setMaster(args(0)).setAppName("Creative")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    spark
  }

  private def searchForNodeAndTag(spark : SparkSession) = {

    val nodeData = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "tag")
      .load("input/oslo.osm")

    nodeData
  }

  def searchForNodeAndTagCityCycling(spark: SparkSession): DataFrame = {

    // Oppretter DataFrame
    val data = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("input/bysykkel.csv")

    data
  }
}

