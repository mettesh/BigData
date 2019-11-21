import org.apache.spark.{SparkConf, SparkContext}

// 5 Linjer med kode (Utenom oppsettet)

object One_BuildingCount {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("Task1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val file = sc.textFile(args(1))

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