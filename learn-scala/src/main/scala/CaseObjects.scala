// A package that demonstrates the uses of case objects
package caseobjects

// Case objects have the following characteristics:
// - serializable
// - have default hashCode implementation
// - have default toString implementation
// - constructor cannot contain parameters
//   These characteristics make case objects good for implementing enumerations
//   and message-like constructs.

// An enumeration of directions
sealed trait Direction
case object North extends Direction
case object South extends Direction
case object East extends Direction
case object West extends Direction

// Message-like constructs using case objects
// These can be sent to a remote-controlled rover
case class ChangeDirection(direction: Direction)    // This has to be a case class because case objects cannot have constructor parameters
case object Go
case object Stop
case object Beep
case class CameraRotate(direction: Direction)
case object CameraOn
case object CameraOff
case object CameraRotateStop

// The client Rover class
case class Rover() {
  def command(cmd: Any): Unit = cmd match {
    case ChangeDirection(direction) => println(s"    Changing direction to $direction")
    case Go => println(s"    Moving forward")
    case Stop => println(s"    Engaging brakes"    )
    case Beep => println(s"    Making noise")
    case CameraRotate(direction) => println(s"    Rotating camera to $direction")
    case CameraOn => println(s"    Turning on camera")
    case CameraOff => println(s"    Turning off camera")
    case CameraRotateStop => println(s"    Stopping camera rotation")
  }
}

object Main {
  def run(): Unit = {
    println("----------------------------------------------------------------------")
    println("  Case Objects")
    println("----------------------------------------------------------------------")
    println("Using case objects for rover commands and directions")
    val rover = Rover()
    rover.command(ChangeDirection(North))
    rover.command(Go)
    rover.command(Stop)
    rover.command(Beep)
    rover.command(CameraRotate(East))
    rover.command(CameraOn)
    rover.command(CameraOff)
    rover.command(CameraRotateStop)

    println()
  }
}