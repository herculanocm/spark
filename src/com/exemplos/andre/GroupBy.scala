package com.exemplos.andre

object GroupBy {
  
  def main(args: Array[String]) {
  val bolos: Seq[String] = Seq("Bolo Chocolate", "Bolo Morango", "Bolo Abacaxi", "Bolo Abacate", "Bolo Cenoura", "Bolo Melancia")
  
  val agrupamentoBolos: Map[Char, Seq[String]] = bolos.groupBy(_.charAt(5))
  
  agrupamentoBolos.foreach(println)
  }
}