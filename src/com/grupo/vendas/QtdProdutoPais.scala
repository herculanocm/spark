package com.grupo.vendas

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object QtdProdutoPais {
  
    def quebraBase(line : String)={
    var linha = line.toUpperCase().replace("Á","A").replace("Ã","A")
    linha = linha.replace("É","E").replace("Ê","E")
    linha = linha.replace("Í","I").replace("Ú","U")
    linha = linha.replace("Ó","O").replace("Õ","O").replace("Ô","O")
    val linhaQuebrada = linha.split(";")
    ((linhaQuebrada(12).trim() + "," + linhaQuebrada(10).trim()), linhaQuebrada(17).trim().toInt)
  }
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "TesteLoja")
    val carregaBase = sc.textFile("C:/PUC/SPPDD/Datasets/Superloja/Superloja.csv")
    //carregaBase.foreach(println)
    
    //extraindo o cabeçalho
    val header = carregaBase.first()
    // removendo o cabeçalho
    val data = carregaBase.filter(row => row != header)  
    
    val mapeiaBase = data.map(quebraBase)
    //mapeiaBase.foreach(println)
    
    val reduzMapeamento = mapeiaBase.reduceByKey((x,y) => x + y)
    
    //val resultadoSemOrdenar = reduzMapeamento.collect()
    //resultadoSemOrdenar.foreach(println)
    
    //inverte os valores da tupla para (Lucro, (ID do produto, País))
    val flipped = reduzMapeamento.map( x => (x._2, x._1) )
    val totalOrdenado = flipped.sortByKey(false)
    //val resultadoOrdenado = totalOrdenado.collect()    
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc) 
    import sqlContext.implicits._
    
    val dataFrameProdutos = totalOrdenado.coalesce(1).toDF()
    //val dataFrameProdutos = reduzMapeamento.coalesce(1).toDF()
    dataFrameProdutos.show()
    
    dataFrameProdutos.write.option("header", "false").csv("./exportacao/dataFrameQtdProdutoPais.csv")    
    
  }   
}