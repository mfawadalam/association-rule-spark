package org.mba



import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by M F Alam on 07/07/2015.
 *
 * Implementation of Association Rule mining
 */
class AssociationRule(rdd: RDD[(Long,Set[String])],sc: SparkContext) {

  val transactionalData: RDD[(Long,Set[String])] = rdd
  val n = rdd.count()
  val products = distinctProducts()

  /*returns support for a single itemset*/
  def support(products: Set[String]): Double ={
    val prodTransactionCounts = transactionalData.map(_._2).filter(x=>products.subsetOf(x)).count().toDouble
    prodTransactionCounts/n
  }

  /*returns support for a single item*/
  def support(product: String): Double ={
    val prodTransactionCounts = transactionalData.map(_._2).filter(x => Set(product).subsetOf(x)).count().toDouble
    prodTransactionCounts/n
  }

  /*returns the lift measure for an association rule*/
  def lift(prodA: Set[String],prodB: Set[String]): Double={
      confidence(prodA,prodB)/support(prodB)
  }

  /*returns the confidence measure for an association rule*/
  def confidence(prodA: Set[String],prodB: Set[String]): Double={
    support(prodA,prodB)/support(prodA)
  }

  /*returns support for an association rule based on two itemsets*/
  def support(prodA: Set[String],prodB: Set[String]): Double={
    val prodTransactionCounts = transactionalData.map(_._2).filter(x=>prodA.subsetOf(x) && prodB.subsetOf(x)).count().toDouble
    prodTransactionCounts/n
  }

  /*distinct products in the transactional data*/
  def distinctProducts(): Set[String]={
    transactionalData.flatMap(_._2).distinct().collect().toSet.filter(_.nonEmpty)
  }

  /*calculates candidate-1 itemsets*/
  def candidateOneItemSets(minSupport: Double): Set[String]={
     products.filter(x=>support(x)>=minSupport)
  }

  /*generates K-frequent itemsets*/
  def generateKFrequentItemSets(k: Int,minSupport: Double): Set[Set[String]] ={
    products.subsets(k).filter(x=>support(x)>=minSupport).toSet
  }

  /*prunes itemset combinations and selects those with support>minsuppor*/
  def prune(k: Int,minSupport: Double): RDD[Set[String]]={
    sc.parallelize({1 to k}.flatMap(x=>generateKFrequentItemSets(x,minSupport)))
  }

  /*runs the association rules over combinations of the pruned itemset*/
  def run(k:Int, minSupport: Double): RDD[(Set[String],Set[String],Double,Double,Double)]={
    val frequentItemSets = prune(k,minSupport).collect()

    sc.parallelize(frequentItemSets.flatMap(f = lhs => frequentItemSets.map { rhs =>
      if (!lhs.equals(rhs))
        (lhs, rhs, support(lhs, rhs), confidence(lhs, rhs), lift(lhs, rhs))
      else
        null
    }).filter(x=>x!=null))

  }

}
