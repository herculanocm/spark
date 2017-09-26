package com.exemplos.andre
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import javassist.runtime.Desc
import org.apache.spark.sql.DataFrame 
import org.dmg.pmml.True

// import scala.collection.parallel.ParIterableLike.Foreach

object MapeandoSegurancaCarros {
  
  
  def QuebraBase(linha : String)={
       
    val quebrabase = linha.split(";")
    

    
    ((quebrabase(3),quebrabase(1),quebrabase(8)),1)
  }
  
   def QuebraBasePeriodo(linha : String)={
       
    val quebrabase = linha.split(";")
    
  //  ((quebrabase(0),quebrabase(1),quebrabase(3),quebrabase(7)),1)
    
    ((quebrabase(1),quebrabase(7)),1)
  }
  

  
  
  def main(args: Array[String]){
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    
   val CONTEXTOSPARK = new SparkContext("local[*]", "MapeandoSegurancaCarros")
   
    val CarregaBase = CONTEXTOSPARK.textFile("c:/sistemas/base.csv")
    
   val Cliente_Remedio = CarregaBase.map(QuebraBase)
   
   val C_Cliente_Remedio = Cliente_Remedio.collect()
   
 
   val quantVendaIbuprofeno = CarregaBase.filter(line => line.contains("ibuprofeno")).count()                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
   println(s"Foram feitas $quantVendaIbuprofeno vendas de Ibuprofeno")
   
   
 val Repeticoes_Cliente_Remedio = Cliente_Remedio.reduceByKey((x,y)=>x+y).sortBy(_._2, false)

  


val C_Repeticoes = Repeticoes_Cliente_Remedio.collect()

val Quant_Rpeticoes = Repeticoes_Cliente_Remedio.count()

println(s"Foram $Quant_Rpeticoes Repetidas")

  
 
  
val Max_Repeticoes = C_Repeticoes.maxBy(_._2)
 
 println(s"Compra mais repetida $Max_Repeticoes")
  
   C_Repeticoes.foreach(println)  
   
 
  }
}
    

