import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CreativeTwo_whereAreTheCyclingStationsInTheExtract {

  // Hvilke 10 steder i extracten er bysykkel mest populært å starte turen fra?

  def main(args: Array[String]) {

    var time = System.currentTimeMillis()

    val spark: SparkSession = initializeSpark(args)
    val osloDataFrame : DataFrame = searchForNodeAndTag(spark)
    val cityCyclingDataFrame : DataFrame = searchForNodeAndTagCityCycling(spark)

    import spark.implicits._

    // Hvor finnes bysykkelstativer i ekstraktet? (Obs, kun de som er blitt brukt så langt i november)

    // Fra oslo.osm plukkes det ut de ulike addr:street-ene som finnes i extractet:
    val placesInExtract = osloDataFrame.select($"_k", $"_v").filter($"_k" === "addr:street")

    //Fra bysykkel.csv plukkes det ut de ulike sykkelstasjonene som er brukt så langt i november
    val startStations = cityCyclingDataFrame.select($"start_station_name", $"start_station_description".as("Lokasjonsbeskrivelse"))

    // Kobler her sammen de to tabellene på adresse/stedsnavnene slik at jeg får kun de stedene som er i ekstraktet:
    val tableJoin = osloDataFrame.join(startStations, placesInExtract("_v") === startStations("start_station_name"))

    // Plukker ut det jeg skal vises i tabellen
    val result = tableJoin.select($"_v".as("Sted i ekstrakt"), $"Lokasjonsbeskrivelse").distinct()

    // Viser 50 av de
    result.show(50)

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

