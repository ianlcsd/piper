/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more
 * details.
 */
package com.clearstorydata

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
  }

}
