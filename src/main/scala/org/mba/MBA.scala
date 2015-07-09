package org.mba

/**
 * Created by sukmof on 06/07/2015.
 */
object MBA {


  def main(args: Array[String]): Unit ={

    val sc = Cluster.getContext("local","MBA App")
    val rdd = sc.textFile(args(0)).map(_.split(" ")).zipWithUniqueId().map(x=>(x._2,x._1.toSet))
    val model = new AssociationRule(rdd,sc)
    model.run(3,0.2).foreach(println)
  }


}
