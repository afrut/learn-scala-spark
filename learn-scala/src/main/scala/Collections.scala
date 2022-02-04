// A package that demonstrates collections in scala
package collections
import scala.collection.mutable.ArrayBuffer

object Main {

  def join(it: Iterable[Any]): String = {
        var msg = ""
        it.foreach((x: Any) => msg += s"${x.toString},")
        s"[${msg.dropRight(1)}]"
  }

  def run(): Unit = {
    println("----------------------------------------------------------------------")
    println("  Collections")
    println("----------------------------------------------------------------------")

    // ArrayBuffer is a collection based on a resizing array.
    println("Create an ArrayBuffer of integers:")
    val abInts = ArrayBuffer[Int](0, 1, 2)                // Create and initialize an ArrayBuffer of ints.
    abInts += 3                                           // Add an element.
    abInts += 4 += 5                                      // Add two more elements
    abInts ++= List(6, 7)                                 // Add a collection of elements
    abInts.append(8)                                      // Add to end
    abInts.appendAll(List(9, 10))                         // Add collection to end
    abInts.prepend(11)                                    // Add to beginning
    abInts.prependAll(List(13, 12))                       // Add to beginning
    println(s"    After additions: ${join(abInts)}")
    abInts -= 3 -= 2                                      // Remove elements 3 and 2
    abInts --= List(1, 7)                                 // Remove all elements in the collection List
    abInts.remove(0)                                      // Remove first element
    abInts.remove(4, 2)                                   // Remove 2 elements starting from the 5th element
    abInts.trimStart(1)                                   // Remove 1 element from start
    abInts.trimEnd(2)                                     // Remove 2 elements from end
    println(s"    After removals: ${join(abInts)}")
    println(s"    abInts(2) = ${abInts(2)}")              // Access 3rd element of array buffer

    // List is an immutable type based on a singly-linked list.
    // Modification will create a new List.
    // NOTE: Since this type is based on a singly-linked list,
    // appending or accessing by index is inefficient. Instead use
    // Vector for an immutable alternative or ArrayBuffer for a
    // mutable alternative.
    println("Creating multiple List collections:")
    val lsInts1 = List(1, 2, 3)                           // Create and initialize a List
    val lsInts2 = 0 +: lsInts1                            // Create an new List and prepend 0
    val lsInts3 = List(4, 5) ++: lsInts2                  // Create yet another List and prepend a collection
    val lsInts4 = 6 :: lsInts3                            // Prepend with ::. Note that lsInts3's last element is :: Nil
    println(s"    lsInts1 = ${join(lsInts1)}")
    println(s"    lsInts2 = ${join(lsInts2)}")
    println(s"    lsInts3 = ${join(lsInts3)}")
    println(s"    lsInts4 = ${join(lsInts4)}")

    // Vector is an immutable type but supports fast access index
    // (maybe resizing array implementation?). Other than this, it's
    // the same as the List collection.
    println("Creating multiple Vector collections:")
    val vInts1 = Vector(1, 2, 3)                          // Create and initialize a List
    val vInts2 = 0 +: vInts1                              // Create an new List and prepend 0
    val vInts3 = Vector(4, 5) ++: vInts2                  // Create yet another List and prepend a collection
    println(s"    vInts1 = ${join(vInts1)}")
    println(s"    vInts2 = ${join(vInts2)}")
    println(s"    vInts3 = ${join(vInts3)}")

    // A map/dictionary equivalent for Scala.
    // Create an immutable Map.
    println("Creating an immutable Map:")
    val imMap = Map(
      "JAN" -> 1,
      "FEB" -> 2,
      "MAR" -> 3
    )
    println(s"    Immutable Map contents: ${join(imMap)}")

    // Create a mutable Map.
    println("Creating a mutable Map:")
    val mapMonths = scala.collection.mutable.Map("JAN" -> 1)
    mapMonths += ("FEB" -> 5)                                        // Add a single entry
    mapMonths ++= Map("APR" -> 4, "MAY" -> 5, "MAR" -> 3)            // Add entries of another map
    println(s"    Mutable Map contents after addition: ${join(mapMonths)}")
    mapMonths -= "SEP"                                               // Remove a single entry
    mapMonths --= List("DEC", "JAN")                                 // Remove a collection of entries
    println(s"    Mutable Map contents after removals: ${join(mapMonths)}")
    mapMonths("FEB") = 2
    println("Loop using for-loop:")
    for((k,v) <- mapMonths) println(s"    $k: $v")                   // Loop through all elements of a map
    println("Loop using foreach and match:")
    mapMonths.foreach {
      case (k,v) => println(s"    $k: $v")                           // Loop using a case statement
    }

    // A set keeps a unique list of elements. It's kind of like a map
    // that keeps only keys.
    // Create an immutable Set.
    println("Creating an immutable Set:")
    val imSt = Set("JAN", "FEB", "MAR")
    println(s"    Immutable Set contents: ${join(imSt)}")

    // Create a mutable Set.
    println("Creating a mutable Set:")
    val setMonths = scala.collection.mutable.Set("JAN", "FEB")
    mapMonths.keys.foreach((x: String) => setMonths.add(x)) // Add keys of mapMonths to setMonths
    setMonths += "MAR"                                      // Add a single element
    setMonths += "APR" += "MAY"                             // Add multiple elements
    setMonths ++= List("AUG", "SEP")                        // Add a collection of elements
    println(s"    Mutable Set contents after additions: ${join(setMonths)}")
    setMonths -= "JUN"                                      // Remove a single element, if present
    setMonths -= "FEB" -= "MAR"                             // Remove multiple elements
    setMonths --= List("APR", "AUG")                        // Remove a collection of elements
    setMonths.remove("JAN")                                 // Remove a single element, if present
    setMonths.remove("MAY")                                 // Remove a single element, if present
    println(s"    Mutable Set contents after removals: ${join(setMonths)}")

    // Some common sequence methods
    println("Some common sequence methods:")
    var msg = ""
    val seq = Seq.range(0, 10)                              // A sequence with the numbers 0 to 9
    val seq1 = (0 to 5).toSeq                               // A sequence with the numbers 0 to 4
    seq.foreach((x) => msg += s"$x,")                       // Iterate through each element and build a csv of the elements
    msg = msg.dropRight(1)                                  // Drop last character of the string
    val doubleSeq = seq.map(_ * 2)                          // Multiply every element in sequence by 2
    val sumDoubleSeq = doubleSeq.reduce(_ + _)              // Reduce all elements by summing them
    val evenSeq = seq.filter(_ % 2 == 0)                    // Return elements whose modulo by 2 is 0
    val head = seq.head                                     // The first element of the sequence
    val tail = seq.tail                                     // All elements but the first element
    val tail5 = seq.tail(5)                                 // The fifth element of the tail of the sequence
    val seqTake3 = seq.take(3)                              // The first 3 elements of the sequence
    val seqTakeWhile = seq.takeWhile(_ < 4)                 // All elements of the sequence less than 4
    val seqDrop3 = seq.drop(3)                              // Remove the first 4 elements
    val seqDropWhile = seq.dropWhile(_ < 4)                 // Remove all elements less than 4
    val seqFoldLeft = seq.tail.foldLeft(seq.head.toString)((x,y) => s"$x,${y.toString}") // Iterate through elements from left to right starting with head and create a csv.
    println(s"    seq = ${seq}")
    println(s"    seq1 = ${seq1}")
    println(s"    seq.foreach = $msg")
    println(s"    doubleSeq = ${doubleSeq}")
    println(s"    sumDoubleSeq = ${sumDoubleSeq}")
    println(s"    evenSeq = ${evenSeq}")
    println(s"    head = ${head}")
    println(s"    tail = ${tail}")
    println(s"    tail5 = ${tail5}")
    println(s"    seqTake3 = ${seqTake3}")
    println(s"    seqTakeWhile = ${seqTakeWhile}")
    println(s"    seqDrop3 = ${seqDrop3}")
    println(s"    seqDropWhile = ${seqDropWhile}")
    println(s"    seqFoldLeft = ${seqFoldLeft}")

    // Some common map methods
    val m = Map(
      1 -> "a",
      2 -> "b",
      3 -> "c",
      4 -> "d"
    )
    println("Some common map methods")
    println("    Iterating over map elements:")
    for((key, value) <- m) println(s"        $key, $value")
    println(s"    All keys: ${m.keys}")
    println(s"    All values: ${m.values}")
    println(s"    May keys contains 5? ${m.contains(5)}")
    println(s"    Transform all values to uppercase: ${m.transform((k, v) => v.toUpperCase)}")
    println(s"    Getting only specified keys: ${m.filterKeys(Set(1, 4)).toMap}")
    println(s"    Taking first 3 elements: ${m.take(3)}")

    val mutm = scala.collection.mutable.Map(
      "AL" -> "Alabama",
      "AK" -> "Alaska"
      )
    println(s"Operations on a mutable map: ${mutm}")
    mutm += ("AZ" -> "Arizona")
    println(s"    Element AZ added: ${mutm}")
    mutm ++= Map("CO" -> "Colorado", "KY" -> "Kentucky")
    println(s"    Elements CO and KY added: ${mutm}")
    mutm -= "AL"
    println(s"    Element AL removed: ${mutm}")
    mutm --= List("AK", "AZ")
    println(s"    Elements AK and AZ removed: ${mutm}")
    mutm("KY") = "Kentucky, fried chicken"
    println("    Modified element KY: "  + mutm("KY"))
    println(s"    Return elements specified by a function: " + mutm.filterInPlace((key, value) => key == "KY"))

    println()
  }
}