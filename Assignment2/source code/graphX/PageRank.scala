package pageRank


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

object PageRank {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GraphX")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, "/csy/edge")

    val ranks = graph.pageRank(0.001).vertices

    val titles = sc.textFile("/id/part-00000").map { line =>
      val fields = line.replace("(", "").replace(")", "").split(",")
      var name = fields(0)
      if (fields.length > 2) {
        for (i <- 1 to fields.length - 2)
          name = name + ", " + fields(i)
      }
      (fields.last.toLong, name)
    }

    val result = titles.join(ranks).map {
      case (id, (title, rank)) => (title, rank)
    }.sortBy(_._2, false)

    result.coalesce(1).saveAsTextFile("/csy/graphX")

    sc.stop()
  }
}
