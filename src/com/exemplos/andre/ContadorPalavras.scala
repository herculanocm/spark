package com.exemplos.andre

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object ContadorPalavras {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     
    val sc = new SparkContext("local", "ContadordePalavras")   
    
    // Load each line of my book into an RDD
    val entrada = sc.textFile("C:/SparkScala/SparkScala/book.txt")
    
    // Faz o split da base usando uma expressão regular que retira palavras
    val palavras = entrada.flatMap(x => x.split("\\W+"))
    
    // Faz tudo ficar minúsculo
    val minusculo = palavras.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    val contagemPalavras = minusculo.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    
    // Inverte (palavra, contagem) para (contagem, palavra) e depois ordena pela chave (contagens)
    val contagemOrdenada = contagemPalavras.map( x => (x._2, x._1) ).sortByKey()
    
    // Mostra os resultados
    for (result <- contagemOrdenada) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
      // s indica que o que será mostrado é do formato string
    }
    
  }
  
}