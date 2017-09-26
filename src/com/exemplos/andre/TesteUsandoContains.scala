package com.exemplos.andre

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object TesteUsandoContains {
  
  def main(args: Array[String]) {
   
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     
    val sc = new SparkContext("local", "ContadordePalavras")   
    
    val entrada = sc.textFile("C:/SparkScala/SparkScalaCourse/src/aula2209/base.csv")
    
    val Mapeia = entrada.map(x=> x.split(";"))
    
    
        
    Mapeia.foreach(println)
    //x(0), x(1)),1)
  }
}