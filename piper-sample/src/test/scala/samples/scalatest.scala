/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more
 * details.
 */
package samples

/*
ScalaTest facilitates different styles of testing by providing traits you can mix
together to get the behavior and syntax you prefer.  A few examples are
included here.  For more information, visit:

http://www.scalatest.org/

One way to use ScalaTest is to help make JUnit or TestNG tests more
clear and concise. Here's an example:
*/
import scala.collection.mutable.Stack
import org.scalatest.Assertions
import org.junit.Test

class StackSuite extends Assertions {

  @Test def stackShouldPopValuesIinLastInFirstOutOrder() {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    assert(stack.pop() === 2)
    assert(stack.pop() === 1)
  }

  @Test def stackShouldThrowNoSuchElementExceptionIfAnEmptyStackIsPopped() {
    val emptyStack = new Stack[String]
    intercept[NoSuchElementException] {
      emptyStack.pop()
    }
  }
}

/*
Here's an example of a FunSuite with ShouldMatchers mixed in:
*/
import org.scalatest.FunSuite
import org.scalatest.Matchers

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
@RunWith(classOf[JUnitRunner])
class ListSuite extends FunSuite with Matchers {

  test("An empty list should be empty") {
    List() should be ('empty)
    Nil should be ('empty)
  }

  test("A non-empty list should not be empty") {
    List(1, 2, 3) should not be ('empty)
    List("fee", "fie", "foe", "fum") should not be ('empty)
  }

  test("A list's length should equal the number of elements it contains") {
    List() should have length (0)
    List(1, 2) should have length (2)
    List("fee", "fie", "foe", "fum") should have length (4)
  }
}

/*
ScalaTest also supports the behavior-driven development style, in which you
combine tests with text that specifies the behavior being tested. Here's
an example whose text output when run looks like:

A Map
- should only contain keys and values that were added to it
- should report its size as the number of key/value pairs it contains
*/
import org.scalatest.MustMatchers
import org.scalatest.FunSpec
class MapSpec extends FunSpec with MustMatchers {

  describe("A Map") {

    it("should only contain keys and values that were added to it") {
      Map("ho" -> 12) must (not contain key ("hi") and not contain value (13))
      Map("hi" -> 13) must (contain key ("hi") and contain value (13))
    }

    it("should report its size as the number of key/value pairs it contains") {
      //Map() must have size (0)
      //Map("ho" -> 12) must have size (1)
      //Map("hi" -> 13, "ho" -> 12) must have size (2)
    }
  }
}

