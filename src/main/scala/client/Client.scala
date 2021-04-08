package client

import client.Util.createStub
import io.grpc.ManagedChannelBuilder
import org.rogach.scallop._
import service.geoService.GeoServiceGrpc.GeoServiceStub
import service.geoService._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

package object Util {
  def createStub(port: Int): GeoServiceStub = {
    val builder =
      ManagedChannelBuilder.forAddress("localhost", port)

    builder.usePlaintext()
    val channel = builder.build()

    GeoServiceGrpc.stub(channel)
  }
}

object Client extends App {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val file = opt[String]()
    val ips = trailArg[String](required = false)
    verify()
  }
  val conf = new Conf(args)

  val ipList = conf.file
    .map { path =>
      val source = Source.fromFile(path)
      val ips = source.getLines().toList
      source.close()
      ips
    }
    .getOrElse {
      if (!conf.ips.isSupplied) {
        System.err.println("ERROR :: No ips supplied")
        sys.exit(1)
      }
      conf.ips.map(_.split(',').toList).getOrElse(List())
    }

  val q = 3
  val initialPort = 50_003

  val stubs = (initialPort until initialPort + q).map(createStub).toList

  val balancer = Balancer(stubs)

  ipList.foreach { ip =>
    balancer.run(_.getLocationByIp(GetLocationByIpRequest(ip))) {
      case Success(GetLocationByIpReply(country, _, _)) => {
        balancer.run(_.getStatesOfCountry(GetStatesOfCountryRequest(country))) {
          case Success(GetStatesOfCountryReply(states, _)) =>
            println(
              s"ip: $ip -> \n\tcountry: $country \n\tand states: ${states
                .mkString("\n\t\t", "\n\t\t", "")}"
            )
          case Failure(_) =>
            System.err.println(
              s"Failed to get states of country $country from ip $ip"
            )
        }
      }
      case _ => System.err.println(s"Failed to get ip: $ip")
    }
  }

  balancer.await()
}

case class Balancer(stubs: List[GeoServiceStub]) {

  private val healthyStubs = mutable.Set[Int]()
  private val workingStubs = mutable.Set[Int]()
  checkHealth()

  private var lastCheck = System.currentTimeMillis()
  private var timeSinceServersDown = 0L
  private var calls = 0
  private var responses = 0

  def await(): Unit = {
    while (calls != responses) print("") // without the print it doesn't work
  }

  private def checkHealth(): Unit = {
    stubs.zipWithIndex.foreach { case (s, i) =>
      val future = s.healthCheck(HealthCheckReq())
      future.onComplete {
        case Success(_) => healthyStubs.add(i)
        case _          => healthyStubs.remove(i)
      }
      Await.ready(future, 500 milliseconds)
    }
  }

  def run[T](
      caller: GeoServiceStub => Future[T]
  )(responder: Try[T] => Unit): Unit = {
    calls = calls + 1

    def runInside(
        caller: GeoServiceStub => Future[T]
    )(responder: Try[T] => Unit)(n: Int): Unit = {

      runAux(caller) {
        case Failure(e) =>
          if (n > 5) {
            responses = responses + 1
            responder(Failure(e))
          } else runInside(caller)(responder)(n + 1)

        case Success(value) =>
          responses = responses + 1
          responder(Success(value))
      }
    }
    runInside(caller)(responder)(0)
  }

  @tailrec
  private final def runAux[T](
      caller: GeoServiceStub => Future[T]
  )(responder: Try[T] => Unit): Unit = {

    if (System.currentTimeMillis() - lastCheck > 500) {
      checkHealth()
      lastCheck = System.currentTimeMillis()
    }

    if (healthyStubs.isEmpty) {
      if (System.currentTimeMillis() - timeSinceServersDown > 2000) {
        System.err.println("ERROR :: All servers are down")
        System.exit(1)
      }
      if (timeSinceServersDown == 0) {
        timeSinceServersDown = System.currentTimeMillis()
      }
      runAux(caller)(responder)

    } else {

      timeSinceServersDown = 0

      val available = stubs.zipWithIndex
        .find { case (_, i) =>
          healthyStubs.contains(i) && !workingStubs.contains(i)
        }

      available match {
        case Some((stub, index)) => {
          workingStubs add index
          val future = caller(stub)
          future.onComplete(_ => workingStubs remove index)
          future.onComplete(responder)
        }
        case None => runAux(caller)(responder)
      }
    }
  }

}
