package com.exemplos.andre

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object Carros_CombinacaoAvaliacoesMaisAltas {
  
  def separaBase( x : String )= {
  
    val base = x.split(",")
    
    //pega preço        
    ( (base(6),base(0),base(1),base(2),base(3),base(4),base(5) ), 1 )
  }
  
  
 
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "Carros3")
    
    val carregaBase = sc.textFile("C:/SparkScala/SparkScala/carros/cardata.csv")
    
    //carregaBase.foreach(println)
    
    val baseMapeada = carregaBase.map(separaBase).filter(x=> x._1._1 == "vgood") // essa linha são resultados mapeados
    
    val reduzIguais = baseMapeada.reduceByKey( (x,y) => x+y)
    
    
    reduzIguais.foreach(println)
    //println(precoAltoAvaliacaoInaceitavel)
    
    
  }
}