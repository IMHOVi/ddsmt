package ru.imho.ddsmt

import akka.actor.{Cancellable, ActorSystem}
import scala.concurrent.duration.FiniteDuration

/**
 * Created by skotlov on 11/21/14.
 */
class Scheduler(actorSystem: ActorSystem) {

  def scheduleOnce(delay: FiniteDuration)(f: => Unit): Cancellable = {
    actorSystem.scheduler.scheduleOnce(delay)(f)(actorSystem.dispatcher)
  }
}
