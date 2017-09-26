package com.grupo.vendas

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.TypedColumn
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._


object Principal {
  
  def quebraVendasPais(line: String) = {
    val separaBase = line.split(";")
     (separaBase(10).toUpperCase().trim(),separaBase(16).replace(",", ".").trim().toFloat)
  }
    

  
  def main(args: Array[String]){
    
    print("Iniciando")
    
    val sc = new SparkContext("local[*]", "Trabalho")
    
    val carregaBase = sc.textFile("./base1/superloja.csv")
    //extraindo o cabeçalho
    val header = carregaBase.first()
    // removendo o cabeçalho
    val data = carregaBase.filter(row => row != header)   
    
       
    val vendaPorPais = data.map(quebraVendasPais)
    
   
    
   vendaPorPais.foreach(println)
    
   val reduzindoVendas = vendaPorPais.reduceByKey( (x,y) => x+y)
    
   
    val sqlContext = new org.apache.spark.sql.SQLContext(sc) 
    import sqlContext.implicits._

   
   val dataFrameVendas = reduzindoVendas.coalesce(1).toDF()
   
  dataFrameVendas.show()
  
  
    dataFrameVendas.write.option("header", "false").csv("./exportacao/dataFrameVendas.csv")
  }
}