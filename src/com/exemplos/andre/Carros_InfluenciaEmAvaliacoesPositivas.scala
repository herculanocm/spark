package com.exemplos.andre

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object Carros_InfluenciaEmAvaliacoesPositivas {
  
  def separaBaseAvaliacaoPreco( x : String )= {
  
    val base = x.split(",")
    
    //pega avaliacao    preco          
    ( (base(6),        base(0)    ), 1 )
  }
  def separaBaseAvaliacaoSeguranca( x : String )= {
  
    val base = x.split(",")
    
    //pega avaliacao    seguranca
    ( (base(6),         base(5) ), 1 )
  }
   def separaBaseAvaliacaoManutencao( x : String )= {
  
    val base = x.split(",")
    
    //pega avaliacao         manutencao      
    ( (base(6),                base(1)  ), 1 )
  }
  
 
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "Carros3")
    
    val carregaBase = sc.textFile("C:/SparkScala/SparkScala/carros/cardata.csv")
    
    //carregaBase.foreach(println)
    
    val baseMapeadaManutencao = carregaBase.map(separaBaseAvaliacaoManutencao) // essa linha são resultados mapeados
    val baseMapeadaPreco = carregaBase.map(separaBaseAvaliacaoPreco)
    val baseMapeadaSeguranca = carregaBase.map(separaBaseAvaliacaoSeguranca)
    
    val filtraManutencaoBaixaOuMedia = baseMapeadaManutencao.filter(x =>  (x._1._2 == "med" || x._1._2 == "low") && (x._1._1 == "good" || x._1._1 == "vgood"))
    
    val reduzManutencaoBaixaOuMedia = filtraManutencaoBaixaOuMedia.reduceByKey( (x,y) => x+y)
    
    val somaManutencaoBaixaOuMedia = reduzManutencaoBaixaOuMedia.map(x => x._2).collect().sum
    
    val filtraPrecoBaixoOuMedio = baseMapeadaPreco.filter(x =>  (x._1._2 == "med" || x._1._2 == "low") && (x._1._1 == "good" || x._1._1 == "vgood"))
    
    val reduzPrecoBaixoOuMedio = filtraPrecoBaixoOuMedio.reduceByKey( (x,y) => x+y)
    
    val somaPrecoBaixoOuMedio = reduzPrecoBaixoOuMedio.map(x => x._2).collect().sum
    
    val filtraSegurancaAlta = baseMapeadaSeguranca.filter(x =>  (x._1._2 == "high") && (x._1._1 == "good" || x._1._1 == "vgood"))
    
    val reduzSegurancaAlta = filtraSegurancaAlta.reduceByKey( (x,y) => x+y)
    
    val somaSegurancaAlta = reduzSegurancaAlta.map(x => x._2).collect().sum
    
    
    
    println("Manutenção baixa ou Média com Avaliações Positivas")
    reduzManutencaoBaixaOuMedia.foreach(println)
    println("Somatório de avaliações positivas com Manutenção baixa ou Média = " + somaManutencaoBaixaOuMedia )
    println("Preço baixo ou Médio com Avaliações Positivas")
    reduzPrecoBaixoOuMedio.foreach(println)
    println("Somatório de avaliações positivas com Preço baixo ou Médio = " + somaPrecoBaixoOuMedio )
    println("Segurança Alta com Avaliações Positivas")
    reduzSegurancaAlta.foreach(println)
    println("Somatório de avaliações positivas com Segurança alta = " + somaSegurancaAlta )
    
  }
}