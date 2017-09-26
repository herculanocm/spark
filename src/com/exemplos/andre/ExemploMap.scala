package com.exemplos.andre

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object ExemploMap {
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "TotalGasto")
    
    val data = sc.textFile("C:/SparkScala/SparkScala/arquivodeTexto.txt")
    
    val mapeandoTxt = data.map (line => line.toUpperCase() )
  
    mapeandoTxt.foreach(println)
    
    val flatMapeandoTxt = data.flatMap( line => line.toUpperCase() )
    println("===========================================================================")
    
    flatMapeandoTxt.foreach(println)
    
    val flatMapeandoTxtPorPalavra = data.flatMap( line => line.split("\\W+") )
    println("===========================================================================")
    
    flatMapeandoTxtPorPalavra.foreach(println)
    
  }
}