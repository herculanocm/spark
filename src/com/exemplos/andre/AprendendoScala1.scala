package com.exemplos.andre

object AprendendoScala1 {
  
  def main(args: Array[String]) {
    
  val hello: String = "oi"                     
  println(hello)                                  
  
 //variáveis ou constantes quando são tipadas, tem o tipo depois do nome
  
  // ao contrário do val, var são variáveis mutáveis
  var helloThere: String = hello                  
  helloThere = hello + " :D!"
  println(helloThere)                             
  
  
  //criando uma constante a partir de outra constante juntamento com uma string
  val immutableHelloThere = hello + " :D!"      
  println(immutableHelloThere)                    
  
  
  val numero : Int = 1                         
  val bool : Boolean = true                      
  val letra : Char = 'a'                       
  val pi : Double = 3.14159265                    
  val piPrecisaoSimples : Float = 3.14159265f     
  val numeroGrande : Long = 1234567890l              
  val numeropequeno : Byte = 127                    
  
  // String printing tricks
  // Concatenating stuff with +:
  println("EXEMPLO DE OUTPUT: " + numero + bool + letra + pi + numeroGrande)
                                                  //> Here is a mess: 1truea3.141592651234567890
  
  
  println(f"Pi é de precisao simples $piPrecisaoSimples%.3f")  //> Pi is about 3.142
  println(f"Adicionando zeros: $numero%05d")
                                                  //> Zero padding on the left: 00001
                                                  
  
  println(s" A letra s pode ser usada para colocar variáveis na mensagem $numero $bool $letra")
                                                  
  println(s"Também é possível incluir uma expressão na saída ${1+2}")
                                                  
                                                 
  
  val ultimaResposta: String = "A resposta para a vida, o universo e tudo mais é 42"
                                                  
  val padrao = """.* ([\d]+).*""".r              
  val padrao(answerString) = ultimaResposta   
  val answer = answerString.toInt                 
  println(answer)                                 
  
  // Booleanos
  val maior = 1 > 2                           
  val menor = 1 < 2                            
  val impossivel = maior & menor           
  val outraforma = maior && menor          
  
  
  }
}