import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Two_AddrStreetTagsPerStreet {

  // How many addr:street tags exist for each street?

  def main(args: Array[String]) {

    var systemTime = System.currentTimeMillis()

    //Initaliserer spark
    val spark: SparkSession = initializeSpark(args)
    //Henter inn data basert på ønskede tagger:
    val nodeData : DataFrame = searchForNodeAndTag(spark)
    import spark.implicits._

    systemTime = System.currentTimeMillis() - systemTime
    printf("Oppstartstid\t: %6.3f s\n", systemTime / 1000.0)

    var time = System.currentTimeMillis()

    // Henter ut attributt k med dens verdi til tag man er i.
    // Plukker ut de taggene hvor attributten er addr:street
    val value = nodeData.select("_k", "_v").filter($"_k" === "addr:street")

    // Grupperer så alle gatene etter verdien. Og teller disse
    val next = value.groupBy($"_v".as("Gatenavn")).count()

    next.show()

    time = System.currentTimeMillis() - time
    printf("Kjøretid i sekunder\t: %6.3f s\n", time / 1000.0)
  }

  private def searchForNodeAndTag(spark : SparkSession) = {
    val nodeData = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "tag")
      .load("input/oslo.osm")

    nodeData
  }

  private def initializeSpark(args: Array[String]) = {
    val conf = new SparkConf().setMaster(args(0)).setAppName("Task 2")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    spark
  }
}