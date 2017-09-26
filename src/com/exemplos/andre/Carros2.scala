package com.exemplos.andre

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


//filtragem usando par preço classe

object Carros2 {
  
  def safety( base : String )= {
  
    val separaCampos = base.split(",")
    
    //pega seguranca       
    (separaCampos(5))
  }
  
  
 
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "Carros")
    
    val carregaBase = sc.textFile("C:/SparkScala/SparkScala/carros/cardata.csv")
    
    
    
    val separaAvaliacoesdeSeguranca = carregaBase.map(safety) // essa linha são resultados mapeados
    
    val contaPorTipo = separaAvaliacoesdeSeguranca.countByValue()
    
    //val coletaContagens = contaPorTipo.toSeq.sortBy(_._1)
    
    
    contaPorTipo.foreach(println)
    
    
    
    
  }
}