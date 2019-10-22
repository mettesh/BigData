import scala.io.Source

object One_BuildingCount {

  def main(args: Array[String]) {

    // How many buildings is it in the extract you selected?
    var buildingCounter = 0;

    val textFile = Source.fromFile("input/oslo.osm").mkString; //returns the file data as String

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