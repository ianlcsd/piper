/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more
 * details.
 */
package com.clearstorydata.piper.core


import java.util.concurrent.ConcurrentHashMap

import akka.actor.{Actor, ActorRef, Props}

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

case class JobRequest[T](parent: ActorRef, job: JobContext => Future[T], id: String)

object Util {
  def log(msg: => String): Unit = {
    println(s"[${Thread.currentThread().getId()}-${System.currentTimeMillis}]: " + msg)
  }
}

/**
  * Bridging the Actor world with Future, but without timeout.
  * We start a response handling actor that completes the future when JobResponse is received.
  * We currently assume that the response handling actor is local and with a shared Map holding
  * (job id, promises), which is also accessible to PiperSubmitter.
  */
object PiperJob {
  val promises = new ConcurrentHashMap[String, Promise[Any]]()
  val responseHandler = PiperContext.jobSystem.actorOf(Props(classOf[ResponseHandler]), "handler")

  def submit[T](id: String, jobContext: JobContext, job: JobContext => Future[T]) = {
    implicit val sender: ActorRef = responseHandler
    responseHandler ! JobRequest(
      jobContext.parent.getOrElse(PiperContext.rootActor),
      job,
      id)
  }

  def apply[T: ClassTag](
    id: String = PiperContext.generateId,
    jobContext: JobContext = PiperContext.defaultJobContext
  )(job: JobContext => Future[T]): Future[T] = {
    val future = promises.computeIfAbsent(
      id,
      new java.util.function.Function[String, Promise[Any]]() {
        override def apply(v1: String): Promise[Any] = Promise[Any]()
      }
    ).future.mapTo[T]
    submit(id, jobContext, job)
    future
  }
}

case class ResponseHandler() extends Actor {
  override def receive: Receive = {
    case JobRequest(delegate, job, id) =>
      delegate ! JobSpawn(job, id)
    case JobFailure(id, ex) =>
      PiperJob.promises.get(id).failure(ex)
      PiperJob.promises.remove(id)
      Util.log(s"job $id failed ")
    case JobSuccess(id, result) =>
      PiperJob.promises.get(id).success(result)
      Util.log(s"job $id done")
      PiperJob.promises.remove(id)
  }
}
