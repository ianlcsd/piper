/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more
 * details.
 */
import akka.actor.ActorSystem

object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    val system = ActorSystem("Barista")
    system.shutdown()
  }

}
