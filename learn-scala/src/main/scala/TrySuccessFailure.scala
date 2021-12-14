// A package that demonstrates the use of Try, Success, and Failure to handle
// exceptions in functional programming
package trysuccessfailure
import scala.util.{Try, Success, Failure}

object Main {
  def run(): Unit = {
    println("----------------------------------------------------------------------")
    println("  Try, Success, Failure")
    println("----------------------------------------------------------------------")
    
    // Try, Success, Failure is very similar to Option, Some, None. They may be
    // valid solutions for the same problem at times. A good practice is to use
    // Option, Some, None for handling null values and to use Try, Success,
    // Failure to handle exceptions.

    // A function with Try, Success, Failure error handling
    def toInt(str: String): Try[Int] = Try(Integer.parseInt(str))
    def matchHelper(str: String): Unit = {
      // Use a match expression to extract the value of wrapped in a Try
      toInt(str) match {
        case Success(x) => println(s"    Successfully converted $x to integer")
        case Failure(x) => println(s"    Failed. Reason: $x")
      }
    }
    println("Try to convert 6 and 6.02 to integer:")
    matchHelper("6")
    matchHelper("6.02")
    println()
  }
}