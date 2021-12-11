// A package that demonstrates the use of Option, Some, and None classes to
// handle null values
package companionobjects

import person.Person

object Main {
  def run(): Unit = {
    println("----------------------------------------------------------------------")
    println("  Companion Objects")
    println("----------------------------------------------------------------------")
    val person1 = new Person()
    println("Constructor that accesses companion object's private fields")
    println(s"    person1 Full Name: ${person1.fullName()}")        // Foo Bar

    // At compile time, the compiler converts this into Person.apply("Jane", "Smith", "F", 27)
    val person2 = Person("Jane", "Smith", "F", 27)
    println("Constructor using apply factory method")
    println(s"    person2 FullName: ${person2.fullName()}")         // Jane Smith
    println(s"    person2 Gender: ${person2.gender}")               // F
    println(s"    person2 Age: ${person2.age}")                     // 27

    // Extract data for person2 using the unapply method in its companion
    // object.
    val (firstName2, lastName2, gender2, age2) = Person.unapply(person2)
    println("Extracting data from person2 using unapply method")
    println(s"    firstName2 = $firstName2")                         // Jane
    println(s"    lastName2 = $lastName2")                           // Smith
    println(s"    gender2 = $gender2")                               // F
    println(s"    age2 = $age2")                                     // 27

    println()
  }
}