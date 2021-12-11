// A package that demonstrates the usefulness of case classes
package caseclasses

// Case classes provide the following features:
// - fields are immutable; accessors are automatically created
// - apply method is automatically created
// - unapply method that returns a tuple of the fields in the constructor is created
// - pattern matching based on the constructor (with unapply method) of the class
// - pattern matching based on type of the class
// - copy, equals, hashCode, and toString are have default implementations

class Motor()
class Blade()
class Mount()
class Ceiling() extends Mount
class Window() extends Mount
class Wall() extends Mount
class Pedestal() extends Mount with Movable
class PortableBase() extends Mount with Movable

trait Fan {
  def motor: Motor  // this is a method
}

trait Movable {}

case class BoxFan(motor: Motor, blade: Blade) extends Fan
case class DysonFan(motor: Motor, mount: PortableBase) extends Fan
case class TableFan(motor: Motor, blade: Blade, mount: PortableBase) extends Fan
case class CeilingFan(motor: Motor, blade: Blade, mount: Ceiling) extends Fan
case class PedestalFan(motor: Motor, blade: Blade, mount: Pedestal) extends Fan
case class WindowFan(motor: Motor, blade: Blade, mount: Window) extends Fan
case class WallFan(motor: Motor, blade: Blade, mount: Wall) extends Fan

object Main {

  def isMovableType(fan: Fan): String = fan match {
    case fan: BoxFan => "movable"
    case fan: DysonFan => "movable"
    case fan: TableFan => "movable"
    case fan: CeilingFan => "NOT movable"
    case fan: PedestalFan => "movable"
    case fan: WindowFan => "NOT movable"
    case fan: WallFan => "NOT movable"
  }

  // Use constructor pattern to check if a fan is movable. This pattern uses the
  // unapply method to provide access to the class' attributes.
  def isMovableConstructorPattern(fan: Fan): String = fan match {
    case BoxFan(motor, blade) => "movable because they don't have mounts"
    case DysonFan(motor, mount) => s"movable because ${mount.getClass.getSimpleName} is movable"
    case TableFan(motor, blade, mount) => s"movable because ${mount.getClass.getSimpleName} is movable"
    case CeilingFan(motor, blade, mount) => s"NOT movable because ${mount.getClass.getSimpleName} is NOT movable"
    case PedestalFan(motor, blade, mount) => s"movable because ${mount.getClass.getSimpleName} is movable"
    case WindowFan(motor, blade, mount) => s"NOT movable because ${mount.getClass.getSimpleName} is NOT movable"
    case WallFan(motor, blade, mount) => s"NOT movable because ${mount.getClass.getSimpleName} is NOT movable"
  }

  // TODO: pattern matching based on if a class extends a trait?
  // TODO: pattern matching based on unapply result?

  def run(): Unit = {
    println("----------------------------------------------------------------------")
    println("  Case Classes")
    println("----------------------------------------------------------------------")
    val boxfan = BoxFan(motor = new Motor(), blade = new Blade())
    val dysonfan = DysonFan(motor = new Motor(), mount = new PortableBase())
    val tablefan = TableFan(motor = new Motor(), blade = new Blade(), mount = new PortableBase())
    val ceilingfan = CeilingFan(motor = new Motor(), blade = new Blade(), mount = new Ceiling())
    val pedestalfan = PedestalFan(motor = new Motor(), blade = new Blade(), mount = new Pedestal())
    val windowfan = WindowFan(motor = new Motor(), blade = new Blade(), mount = new Window())
    val wallfan = WallFan(motor = new Motor(), blade = new Blade(), mount = new Wall())

    println("Case class pattern matching based on type")
    println(s"    boxfan: ${isMovableType(boxfan)}")
    println(s"    dysonfan: ${isMovableType(dysonfan)}")
    println(s"    tablefan: ${isMovableType(tablefan)}")
    println(s"    ceilingfan: ${isMovableType(ceilingfan)}")
    println(s"    pedestalfan: ${isMovableType(pedestalfan)}")
    println(s"    windowfan: ${isMovableType(windowfan)}")
    println(s"    wallfan: ${isMovableType(wallfan)}")

    println("Case class pattern matching based on constructor pattern")
    println(s"    boxfan: ${isMovableConstructorPattern(boxfan)}")
    println(s"    dysonfan: ${isMovableConstructorPattern(dysonfan)}")
    println(s"    tablefan: ${isMovableConstructorPattern(tablefan)}")
    println(s"    ceilingfan: ${isMovableConstructorPattern(ceilingfan)}")
    println(s"    pedestalfan: ${isMovableConstructorPattern(pedestalfan)}")
    println(s"    windowfan: ${isMovableConstructorPattern(windowfan)}")
    println(s"    wallfan: ${isMovableConstructorPattern(wallfan)}")

    println()
  }
}