import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Two_AddrStreetTagsPerStreet {

  // How many addr:street tags exist for each street?
  // 3 Linjer med kode utenom oppsettet

  def main(args: Array[String]) {

    //Initaliserer spark
    val spark: SparkSession = initializeSpark(args)

    var time = System.currentTimeMillis()

    //Henter inn data basert på ønskede tagger:
    val nodeData : DataFrame = searchForNodeAndTag(spark)

    import spark.implicits._

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
    val conf = new SparkConf().setMaster(args(0)).setAppName("Two - How many addr:street tags exist for each street?")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    spark
  }
}