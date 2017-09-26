package com.exemplos.andre

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.catalyst.expressions.Coalesce

object GroupByUsandoBase {
  
  def main(args: Array[String]) {
  
   Logger.getLogger("org").setLevel(Level.ERROR) 
  
   val sc = new SparkContext("local[*]", "GroupByUsandoBase")
    
   val data = sc.textFile("C:/SparkScala/SparkScala/ArquivoParaAgrupar.txt")
  
  
   val mapeandoBase = data.map( x =>  (x.split(" ")(0),x.split(" ")(1) ) )
   
   val coletaBaseMapeada = mapeandoBase.collect().groupBy(x  => (x._2.charAt(0), (x._2) )  )
//   
//   
    
   println(" Mostrando a base mapeada ")
    mapeandoBase.foreach(println)
    println()
    println(" Mostrando base agrupada pela primeira letra do tipo de bolo")
    coletaBaseMapeada.foreach(println)
    
  }
}