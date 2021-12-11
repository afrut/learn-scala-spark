package person

import scala.util.Random

// A user-defined class.
// Arguments of the constructor define the fields of the class.
// If fields are declared with val, they will be immutable.
class Person(var firstName: String
  ,var lastName: String
  ,var gender: String) {

  // An overloaded/auxiliary constructor. An auxiliary constructor must call the
  // default constructor.
  def this(firstName: String, lastName: String) {
    this(firstName, lastName, "X")
  }

  // Another overloaded/axuiliary constructor using the companion object's
  // private fields.
  def this() {
    this(Person.defaultFirstName, Person.defaultLastName, Person.defaultGender)
  }

  // Public fields.
  var age = 0

  // A private field
  private val socialInsuranceNumber = 100000 + (new Random).nextInt(999999 - 100000)

  def fullName() = s"$firstName $lastName"
  def printFullName() = println(s"$firstName $lastName")
}

// A companion object of a class is an object declared in the same file as the
// class with the same name as the class.
object Person {

  // The class and its companion object can access each other's private members.
  private val defaultFirstName = "Foo"
  private val defaultLastName = "Bar"
  private val defaultGender = "X"

  // A companion object's apply method enables the instantiation of the class
  // without the new keyword. apply is a factory method. Multiple apply methods
  // be overloaded to create different constructors.
  def apply(firstName: String, lastName: String, gender: String, age: Int) = {
    var person = new Person
    person.firstName = firstName
    person.lastName = lastName
    person.gender = gender
    person.age = age
    person
  }

  // An unapply method deconstructs the instance of the class and returns data
  // for that class. In the example below, a Tuple4[String, String, String, Int]
  // is returned. This can be used to "extract" data from an instance of an class.
  def unapply(person: Person): Tuple4[String, String, String, Int] =
    (person.firstName, person.lastName, person.gender, person.age)
}