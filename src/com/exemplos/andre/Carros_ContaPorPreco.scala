package com.exemplos.andre

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object Carros_ContaPorPreco {
  
  def pegaPreco( base : String )= {
  
    val separaCampos = base.split(",")
    
    //pega preço        
    ( separaCampos(0)  )
  }
  
  
 
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "Carros3")
    
    val carregaBase = sc.textFile("C:/SparkScala/SparkScala/carros/cardata.csv")
    
    //carregaBase.foreach(println)
    
    val mapeiaPrecos = carregaBase.map(pegaPreco).countByValue() // essa linha são resultados mapeados
    
    
    
    
    mapeiaPrecos.foreach(println)
    //println(precoAltoAvaliacaoInaceitavel)
    
    
  }
}