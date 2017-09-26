package com.exemplos.andre

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._




object TesteContagem {
  
  
  
 //A função divideCsv recebe um x do tipo String, quebra cada linha desse x onde existirem vírgulas, por exemplo:
 // uma entrada como essa     123, 123, 123    ->> seria quebrada em três campos
 // posteriormente, escolhemos uma maneira de pegar desses campos somente o que for de interesse e formatamos a maneira com a qual o x vai ser devolvido
 def divideCsv( x : String) ={
    
    val separaCampos = x.split(",")
    
    val cliente = separaCampos(1)
    val produto = separaCampos(2)
    val preco = separaCampos(3)
    (produto)
    
  }
  
  
  def main(args: Array[String]) {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
        
// Create a SparkContext using every core of the local machine, named RatingsCounter
// a constante sc é quem vai ser responsável por criar um RDD (resilient distributed dataset)
// dissemos que o cluster vai ser executado localmente, usando todos os cores, em um objeto scala chamado TesteContagem
  
  val sc = new SparkContext("local[*]", "TesteContagem")
   
// criamos uma segunda constante lines, essa constante recebe cada linha lida a partir de um csv 
// para isso usamos a nossa constante anterior "sc" que foi criada como um SparkContext
//Definição de spark context pela apache: 
//A SparkContext represents the connection to a Spark cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
// Permite criar um RDD de diversas maneiras, vindo de csv, vindo do cassandra, vindo de um HDFS, etc, etc, etc
  
  val lines = sc.textFile("C:/SparkScala/SparkScala/SalesJan2009(somentedados).csv")
    
    
  val mapeandoProduto = lines.map(divideCsv)
  
  val coletandoMapeamentos = mapeandoProduto.collect()
  
  coletandoMapeamentos.foreach(println)
  
 
  
  }
  
  
}