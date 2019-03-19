import scala.annotation.tailrec

object TailRecursion {

  def main(args: Array[String]): Unit = {
    println(factorial(70000, 1))
  }



//  @tailrec
  def factorial(n:Int):Long={if (n <= 0) 1 else n +  factorial(n -1)}

  def factorial(n:Long, acc:Long):Long ={if (n <= 0) acc else factorial(n -1 , acc + n) }



}
