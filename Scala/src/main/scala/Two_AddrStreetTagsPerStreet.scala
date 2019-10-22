import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Two_AddrStreetTagsPerStreet {

    def main(args: Array[String]) {
      val conf = new SparkConf()
      val sc = new SparkContext(conf)
      val file = sc.textFile("input/oslo.osm")
      val wordRdd = file.flatMap( line => line.split(" ") ).map( word => (word, 1) )

      val counts = wordRdd.reduceByKey( _ + _ )

      println(counts)

      counts.saveAsTextFile("output/counts.txt")
    }
}