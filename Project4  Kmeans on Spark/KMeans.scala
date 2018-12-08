import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object KMeans {

  type Point=(Double,Double)
  var centroids: Array[Point] = Array[Point]()



  def distance (p: Point, point: Point): Double ={

    var distance = Math.sqrt ((p._1 - point._1) * (p._1 - point._1) + (p._2 - point._2) * (p._2 - point._2) );
    distance
  }

  def main (args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)

    centroids = sc.textFile(args(1)).map( line => { val a = line.split(",")
      (a(0).toDouble,a(1).toDouble)}).collect
    var points=sc.textFile(args(0)).map(line=>{val b=line.split(",")
      (b(0).toDouble,b(1).toDouble)})

    for(i<- 1 to 5){
      val cs = sc.broadcast(centroids)
      centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }
        .groupByKey().map { case(c,pointva)=>
        var count=0
        var sumX=0.0
        var sumY=0.0

        for(ps <- pointva) {
           count += 1
           sumX+=ps._1
           sumY+=ps._2
        }
        var centroidx=sumX/count
        var centroidy=sumY/count
        (centroidx,centroidy)

      }.collect
    }

centroids.foreach(println)
    }









}
