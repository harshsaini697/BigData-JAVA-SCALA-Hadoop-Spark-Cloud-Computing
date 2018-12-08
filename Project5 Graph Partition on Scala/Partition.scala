import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {
  var i=0
  val depth = 6
  def main ( args: Array[ String ] ) {
    val sc = new SparkContext(new SparkConf().setAppName("Partition"))
    var graph = sc.textFile("small-graph.txt").map(line=>{val a = line.split(",")
      i=i+1
      var list:List[Long]=List[Long]()
      var x=""
      for(x<-a){list=list:+x.toLong}
      if(i<=5){(a(0).toLong,a(0)+"",list.tail)}
      else{(a(0).toLong,Long(-1),list.tail) }})

    for(i<-1 to depth){
      var graph2=graph.map(line=>{
        (line._1,Left(line._2,line._3))

        (line._1,Right(line._2))
      })
    }.groupByKey.map{

        var adjacent=0
        var cluster=(-1)
        for(p <- Left)
          if (p == Right(c))
            cluster = c
        if (p == Left(c,adj) && c>0)
          (id,c,adj)   /* the node has already been assigned a cluster */
        if (p== Left(-1,adj))
          adjacent = adj
      ( id, cluster, adjacent )
      }.saveAsTextFile("output")
  }
}
