package org.mba

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sukmof on 06/07/2015.
 */
object Cluster {
  def getContext(url: String,appName: String): SparkContext={
    val conf = new SparkConf().setAppName(appName).setMaster(url)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc
  }

}
