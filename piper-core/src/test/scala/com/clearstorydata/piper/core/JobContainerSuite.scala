/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more
 * details.
 */
package com.clearstorydata.piper.core

import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent._
import scala.util.Try

class JobContainerSuite extends FunSuite with Matchers {

  test("Future extension") {
    trait FutureAction[T] extends Future[T] {
      def cancel() = Util.log(s"cancelling")
    }

    object FutureAction {
      def apply[T](body: =>T)(implicit execctx: ExecutionContext): FutureAction[T] = {
        val f = Future(body)(execctx)

        new FutureAction[T] {
          override def isCompleted: Boolean = f.isCompleted

          def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext) = {
            f.onComplete(func)(executor)
          }

          override def value: Option[Try[T]] = f.value

          override def result(atMost: Duration)(implicit permit: CanAwait): T = {
            f.result(atMost)(permit)
          }

          override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
            f.ready(atMost)(permit)
            this
          }
        }
      }
    }

    // The type is worng, it should be FutureAction[String]
    val result: Future[String] = PiperJob() {
      context: JobContext => {
        val bbb: FutureAction[String] = FutureAction{"FutureAction"}
        bbb
      }
    }


  }
  test("Failure Propagation") {
    val f1 =
      PiperJob() {
        context: JobContext =>
          PiperJob(
            id = PiperContext.generateId,
            jobContext = context) { context: JobContext =>
            PiperJob(
              id = PiperContext.generateId,
              jobContext = context) { context: JobContext =>
              Future {
                1000
              }
            }.map(_ + 100/0)
          }.map(_ + 10)
      }

    intercept[ArithmeticException] {
      Await.result(f1, 20 seconds)
    }

    // wait a while for the actors to die
    Thread.sleep(50)
    val r3 = Await.result(PiperContext.count, 10 seconds)
    assert(r3 == 0)

  }

  test("Simple Test") {
    val f1 =
      PiperJob() {
        context: JobContext =>
          PiperJob(
            id = PiperContext.generateId,
            jobContext = context) { context: JobContext =>
            PiperJob(
              id = PiperContext.generateId,
              jobContext = context) { context: JobContext =>
              Future {
                1000
              }
            }.map(_ + 100)
          }.map(_ + 10)
      }

    val res1 = Await.result(f1, 20 seconds)
    assert(1110 == res1)
    val r3 = Await.result(PiperContext.count, 10 seconds)
    assert(r3 == 0)
  }

  test("Fibonacci Job") {
    def feb1(n: Int): Int = n match {
      case 1 => 1
      case 2 => 1
      case _ => feb1(n - 2) + feb1(n - 1)
    }

    def feb2(n: Int, context: JobContext): Future[Int] = {
      PiperJob(
        id = n.toString,
        jobContext = context) { context: JobContext =>
        n match {
          case 1 => Future{1}
          case 2 => Future{1}
          case _ =>
            for {
              f1 <- feb2(n - 2, context)
              f2 <- feb2(n - 1, context)
            } yield f1 + f2
        }
      }
    }
    val n = 20
    val r1 = feb1(n)
    val r2 = Await.result( feb2(n, PiperContext.defaultJobContext), 1 hour)

    println(s"r1=${r1}, r2=${r2}")
    assert(r1 == r2)

    val r3 = Await.result(PiperContext.count, 15 seconds)
    assert(r3 == 0)
  }

}
