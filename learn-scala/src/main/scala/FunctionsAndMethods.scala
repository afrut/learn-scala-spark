// A package that demonstrates scala functions and methods
package functionsandmethods
import java.util.Calendar
import java.text.SimpleDateFormat
import scala.util.Random

object Main {
  val rand = new Random

   // A method with no return value.
  def log(msg: String): Unit = {
    val dt = Calendar.getInstance().getTime()
    val fmt = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
    println(s"${fmt.format(dt)}: $msg")
  }

  // A method to determine of an integer is odd or even.
  def oddOrEven(p: Int): Boolean = p % 2 match {
    case 0 => true
    case _ => false
  }

  // Equation of a line. Compute y given, slope m, intercept b, and x.
  def line(m: Double, b: Double)(x: Double): Double = {(m * x) + b}

  // Get a random number between start and stop.
  def getRandomInt(start: Int, stop: Int) = {rand.nextInt(stop - start) + start}
  def getRandomInt(stop: Int) = {rand.nextInt(stop)}
  def getRandomInt() = {rand.nextInt(20)}

  // Sum up an arbitrary number of values
  def sum(a: Double*) = {
    var ret: Double = 0
    a.foreach(x => ret += x)
    ret
  }

  def run(): Unit = {
    println("----------------------------------------------------------------------")
    println("  Functions and Methods")
    println("----------------------------------------------------------------------")
    
    val a = 10
    var b = 2
    println(s"a = $a")
    println(s"b = ${b}") 
    log(s"a = $a")

    // Define some anonymous functions within a function.
    val timesTwo = (x: Int) => x * 2
    val add = (x: Int, y: Int) => x + y
    println(s"timesTwo(a) = ${timesTwo(a)}")
    println(s"add(a, b) = ${add(a, b)}")

    // An anonymous function with specified return type
    val toString = (x: Any) => {x.toString} :String
    println(toString(toString))

    // Define a method within a function.
    // Equation of a line.
    def f(m: Double, b: Double)(x: Double): Double = {
      (m * x) + b         // last line is the return
    }
    println(s"f(2,1)(a) = ${f(2,1)(a)}")

    // Use a function with a variable number of arguments
    // Use the splat operator on a collection to "unpack" it
    val somenums = Seq(4.0, 5.0, 6.0)
    println(s"sum(1, 2, 3, 4, 7, 11) = ${sum(1, 2, 3, 4, 7, 11)}")
    println(s"sum(${somenums}) = ${sum(somenums:_*)}")

    println()
  }
}