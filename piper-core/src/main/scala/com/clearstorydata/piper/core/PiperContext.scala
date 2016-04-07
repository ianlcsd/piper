/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more
 * details.
 */

package com.clearstorydata.piper.core

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

case class JobContext(parent: Option[ActorRef])

case class JobSpawn[T](job: JobContext => Future[T], id: String)

trait JobResult[+T]

case class JobSuccess[+T](id: String, result: T) extends JobResult[T]

case class JobFailure[+T](id: String, ex: Throwable) extends JobResult[T]

case class JobRun[T](job: JobContext => Future[T], id: String)

case object JobCountRequest

case class JobCountResult(count: Int)

object DefaultJobContext

object PiperContext {
  val rootString = "root"
  val jobSystem: ActorSystem = ActorSystem("piper")
  val rootActor: ActorRef = PiperContext.jobSystem.actorOf(Props(classOf[JobRootActor]), rootString)
  val defaultJobContext = JobContext(parent = None)

  def generateId = UUID.randomUUID().toString

  def count: Future[Int] = {
    implicit val timeout = Timeout(10000)
    rootActor ? JobCountRequest
  }.mapTo[JobCountResult].map(_.count)
}


trait PiperActor extends Actor with ActorLogging {
  import akka.actor.SupervisorStrategy.{Directive, Stop}
  import akka.actor.{OneForOneStrategy, Props, SupervisorStrategy}
  import scala.concurrent.duration._

  /**
    * Stop when exception thrown
    */
  val piperDecider: PartialFunction[Throwable, Directive] = {
    case _: Exception => Stop
  }

  /**
    * Retry 3 times in 2 minutes and stop
    */
  override def supervisorStrategy =
    OneForOneStrategy(3, 2.minutes){(piperDecider.orElse(SupervisorStrategy.defaultStrategy.decider))}

  def spawning: Receive = {
    case JobSpawn(job, id) =>
      Try {
        val childActor = context.actorOf(Props(classOf[JobActor]), id)
        childActor.forward(JobRun(job, id))
      } match {
        case Success(_) => // do nothing
        case Failure(ex) => sender ! JobFailure(id, ex)
      }
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(s"${reason} is caught restarting")
  }
}

/**
  * RootActor only spawns up JobActors
  */
case class JobRootActor() extends PiperActor with ActorLogging {
  override def receive: Receive = counting orElse spawning

  def counting: Receive = {
    case JobCountRequest => sender ! JobCountResult(context.children.size)
  }
}

/**
  * JobActor models the stages and transitions of a Job Running
  */
case class JobActor() extends PiperActor with ActorLogging {

  override def receive: Receive = waiting

  def running: Receive = {
    case (result: JobSuccess[Any], actor: ActorRef) =>
      actor ! result
      context.stop(self)
    case (result: JobFailure[Any], actor: ActorRef) =>
      actor ! result
      context.stop(self)
  }

  def runningAndSpawning = running orElse spawning

  def waiting: Receive = {
    case JobRun(job, id) =>
      val p = Promise[(JobResult[Any], ActorRef)]()
      val requester = sender
      (for {
        jc <- Future(JobContext(parent = Some(self)))
        j <- job(jc)
      } yield j).onComplete {
        case Success(r) => p.complete(Try((JobSuccess(id, r), requester)))
        case Failure(t) => p.complete(Try((JobFailure(id, t), requester)))
      }
      p.future.pipeTo(self)
      context.become(runningAndSpawning)
  }
}
