import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object One_BuildingCount {

  def main(args: Array[String]) {

    //val conf = new SparkConf().setAppName("Simple Application")
    //val sc = new SparkContext(conf)
    //val file = sc.textFile("/Users/Mette/Documents/skole/bigdata/BigDataProsjekt/Scala/src/main/scala/input/oslo.osm")
    //val counts = file.flatMap( line => line.split(" ") ).map( word => (word, 1) ).reduceByKey()
    
    // How many buildings is it in the extract you selected?
    var buildingCounter = 0;

    val textFile = Source.fromFile("/Users/Mette/Documents/skole/bigdata/BigDataProsjekt/Scala/src/main/scala/input/oslo.osm").mkString; //returns the file data as String

    //splitting String data with white space
    val words = textFile.split(" ")

    for( aWord <- words ){
      if(aWord.equals("k=\"building\"")){
        buildingCounter = buildingCounter + 1
      }
    }

    println("Numbers of buildings in the extract i selected: " + buildingCounter)

  }

}