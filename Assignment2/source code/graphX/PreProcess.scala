package pageRank

import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.{NodeSeq, XML}
import org.apache.spark.graphx.VertexId


object GraphPageRank {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("GraphX")
    val sc = new SparkContext(conf)

    val iters = if (args.length > 1) args(1).toInt else 10
    val lines = sc.textFile(args(0))
    val edges= lines.map ( line => {
      val fields = line.split("\t")
      val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
      val targets =
        if (body == "\\N") {
          NodeSeq.Empty
        } else {
            XML.loadString(body) \\ "link" \ "target"
        }
      val targetsText = targets.map(target => new String(target.text.toLowerCase())).toArray
      (new String(title).toLowerCase(), targetsText)
    }).flatMap(pair =>
      pair._2.map(arrayElem => (pair._1, arrayElem))
    ).cache()

    var index:VertexId = 0L
    val id = lines.map( line =>  {
      val title = line.split("\t")(1)
      index = index + 1L
      (title.toLowerCase(), index)
    }).cache()

    val indexTitle = sc.broadcast(id.collectAsMap())

    val relation = edges.map(edge => {
      if (indexTitle.value.contains(edge._1))
        if (indexTitle.value.contains(edge._2))
          indexTitle.value(edge._1) + " " + indexTitle.value(edge._2)
    })

    relation.saveAsTextFile("/csy/edge")

    sc.stop()
  }
}

