import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Two_AddrStreetTagsPerStreet {

  def main(args: Array[String]) {

    val spark: SparkSession = initializeSpark(args)
    val nodeData : DataFrame = searchForNodeAndTag(spark)

  }

  private def searchForNodeAndTag(spark : SparkSession) = {
    val nodeData = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "node")
      .option("rowTag", "tag")
      .load("input/oslo.osm")

    nodeData
  }

  private def initializeSpark(args: Array[String]) = {

    val conf = new SparkConf().setMaster(args(0)).setAppName("Task2")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    spark
  }
}