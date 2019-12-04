import org.apache.spark.{SparkConf, SparkContext}

object One_BuildingCount {

  def main(args: Array[String]) {

    var systemTime = System.currentTimeMillis()

    val conf = new SparkConf().setMaster(args(0)).setAppName("Task 1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val file = sc.textFile(args(1))

    systemTime = System.currentTimeMillis() - systemTime
    printf("Oppstartstid\t: %6.3f s\n", systemTime / 1000.0)

    var time = System.currentTimeMillis()

    val buildingCounter = file.flatMap(line => line.split(" ") )
      .map(word => (word.equals("k=\"building\""), 1))
      .reduceByKey( _ + _ )
      .map(word => "building" + word._1 + ": " + word._2)

    buildingCounter.saveAsTextFile(args(2))

    time = System.currentTimeMillis() - time
    printf("Kjøretid i sekunder\t: %6.3f s\n", time / 1000.0)


  }
}