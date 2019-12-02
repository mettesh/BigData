import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Three_ObjectMostUpdated {

  // Which object in the extract has been updated the most times, and what object is that

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

    // Henter id og versjon fra noder som har disse
    val select = nodeData.select($"_id".as("Id"), $"_version".as("Version"))

    // Sorterer disse synkede basert på versjonsnummer
    val versions = select.orderBy($"_version".desc)

    //Skriver ut den første linjen (Som oså har det høyeste tallet)
    versions.show(1)

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

  private def initializeSpark(args: Array[String]) = {
    val conf = new SparkConf().setMaster(args(0)).setAppName("Task 3")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    spark
  }
}
