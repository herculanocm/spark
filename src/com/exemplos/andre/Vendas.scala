package com.exemplos.andre

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object Vendas{
  
  def divideCsv( linhas:String) ={
    
    val separaCampos = linhas.split(",")
    
    val cliente = separaCampos(0)
    val produto = separaCampos(1)
    val preco = separaCampos(2)
    
    ( cliente.toInt, preco.toFloat)
    
  }
  def produtoMaisVendido( linhas:String) ={
    
    val separaCampos = linhas.split(",")
    
    val cliente = separaCampos(0)
    val produto = separaCampos(1)
    val preco = separaCampos(2)
    
    ( produto, 1)
    
  }
  
    def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "Vendas")
   
    
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("C:/SparkScala/SparkScala/customer-orders.csv")
    
    val rdd = lines.map(divideCsv)
    
    val reduzClienteGastos = rdd.reduceByKey( (x,y) => x+y )
    
    val ordenaClienteGastos = reduzClienteGastos.sortByKey()
    
    val coleta = ordenaClienteGastos.collect()
    println("-----------------------------Clientes e seus Respectivos Gastos-----------------------------------------------")
    coleta.foreach(println)
    
    
    val rdd2 = lines.map(produtoMaisVendido)
    
    val reduzProdutoMaisVendido = rdd2.reduceByKey( (x,y) => x+y )
    
    val inverteChaveValor = reduzProdutoMaisVendido.map( x => (x._2, x._1))
    
    val ordenaMaisVendidos = inverteChaveValor.sortByKey()
    
    val coleta2 = ordenaMaisVendidos.collect()
    
    println("-----------------------------Produtos mais Vendidos-----------------------------------------------")
    coleta2.foreach(println)
    //("C:/SparkScala/SparkScala/testeRdd(doisfiltros).txt")
    }
  
  
}

//Transaction_date,Product,Price,Payment_Type,Name,City,State,Country,Account_Created,Last_Login,Latitude,Longitude