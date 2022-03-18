// A package that demonstrates classes in scala
package classes

import person.Person

object Main {
  def run(): Unit = {
    println("----------------------------------------------------------------------")
    println("  Classes")
    println("----------------------------------------------------------------------")

    // Create an instance of type Person.
    val person1 = new Person("John", "Smith")
    println("Created new instance of Person person1:")
    println(s"    person1: ${person1.toString}")
    println(s"    person1 Full Name: ${person1.fullName()}")

    // Determine the type of variable
    println(s"    The type of person1 is ${person1.getClass}")
    println(s"    The type of person1 is ${person1.getClass.getName}")
    println(s"    The type of person1 is ${person1.getClass.getSimpleName}")

    // Create an instance of Person with named parameters.
    val person2 = new Person(lastName = "Smith", firstName = "John")
    println("Created new instance of Person person2 using named parameters:")
    println(s"    person2: ${person2.toString}")
    println(s"    person2 Full Name: ${person2.fullName()}")
    println(s"    class of person2: ${person2.getClass}")

    // Get the object representing a type.
    println(s"Class of person: ${classOf[Person]}")

    println()
  }
}