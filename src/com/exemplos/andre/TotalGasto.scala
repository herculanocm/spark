package com.exemplos.andre



import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Compute the total amount spent per customer in some fake e-commerce data. */
object TotalGasto {
  
  /** Converte a entrada em  tuplas -> (IDcliente, QuantidadeGasta)  */
  def extractCustomerPricePairs(line: String) = {
    var fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalGasto")   
    
    val entrada = sc.textFile("C:/SparkScala/SparkScala/customer-orders.csv")
    
    val entradaColetada= entrada.collect()
    
    //entrada.coalesce(1).saveAsTextFile("C:/salvaBaseTotalGasto.txt")
    
    val entradaMapeada = entrada.map(extractCustomerPricePairs)
    
    //até aqui temos a tupla com (Idcliente, totalGasto)
    val totalPorCliente = entradaMapeada.reduceByKey( (x,y) => x + y )
    
    val resultadoSemOrdenar = totalPorCliente.collect()
    
    //aqui invertemos os valores da tupla para (TotalGasto, IdCliente)
    val flipped = totalPorCliente.map( x => (x._2, x._1) )
    
    val totalOrdenado = flipped.sortByKey()
    
    val resultadoOrdenado = totalOrdenado.collect()
    
    println("ID_cliente  Preço")
    resultadoSemOrdenar.foreach(println)
    println("--------------------------------------------------------------------------------------------------------------------")
    println("Preço   ID_cliente")
    resultadoOrdenado.foreach(println)
    println("--------------------------------------------------------------------------------------------------------------------")
    
    // Print the results.
    //resultado.foreach(println)
  }
  
}
