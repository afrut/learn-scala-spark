// A package that demonstrates the use of Option, Some, and None classes to
// handle null values
package optionsomenone

object Main {
  def run(): Unit = {
    println("----------------------------------------------------------------------")
    println("  Option, Some, None")
    println("----------------------------------------------------------------------")
    
    // In functional programming, some operations will encounter errors with
    // incorrect answers. Examples include converting strings to int and
    // division. A problem arises when the oepration fails - what value should
    // be returned? A solution is to return an option instead. When the
    // operation succeeds, the function returns Some(ret). When the operation
    // fails, the function returns None.

    def toInt(str: String): Option[Int] = {
      try {
        // Attempt to return the quotient wrapped in Some() option.
        Some(Integer.parseInt(str.trim))
      } catch {
        // Error is encountered, return None option.
        case e: Exception => None
      }
    }

    // The Option-Some-None construct can be used to convey optionality of
    // function parameters.
    def range(start: Option[Int], end: Int) = {
      start match {
        case Some(x) => List.range(x, end)
        case None => List.range(0, end)
      }
    }

    println("Converting 6 to Int:")
    toInt("6") match {
      case Some(ret) => println(s"    Successfully converted to int $ret")
      case None => println("    Error converting to int")
    }

    println("Converting 6.02 to Int:")
    toInt("6.02") match {
      case Some(ret) => println(s"    Successfully converted to int $ret")
      case None => println("    Error converting to int")
    }

    println("Range with start value")
    println(s"    range(Some(5), 10) = ${range(Some(5), 10)}")
    println("Range with no start value:")
    println(s"    range(None, 10) = ${range(None, 10)}")

    println()
  }
}