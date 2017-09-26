package com.exemplos.andre

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._




object Carros {
  
  def par_Preco_Resultado( base : String )= {
  
    val separaCampos = base.split(",")
    
    //pega preço       pega classe 
    (separaCampos(0), separaCampos(6))
  }
  
  
 
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "Carros")
    
    val carregaBase = sc.textFile("C:/SparkScala/SparkScala/carros/cardata.csv")
    
    //carregaBase.foreach(println)
    
    val separaParesPrecoResultado = carregaBase.map(par_Preco_Resultado) // essa linha são resultados mapeados
    
    val coletaParesSeparados = separaParesPrecoResultado.collect()  // essa linha são resultados já coletados
    
    //coletaParesSeparados.foreach( println)
    
    val precoAltoAvaliacaoInaceitavel = coletaParesSeparados.filter(x => (x._1 == "vhigh" && x._2 == "acc")).map( x => 1)//.sum
    
    precoAltoAvaliacaoInaceitavel.foreach(println)
    //println(precoAltoAvaliacaoInaceitavel)
    
    
  }
}