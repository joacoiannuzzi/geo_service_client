package client

import io.grpc.ManagedChannelBuilder
import service.geoService.GeoServiceGrpc.{GeoServiceStub, stub}
import service.geoService.{
  GeoServiceGrpc,
  GetLocationByIpReply,
  GetLocationByIpRequest,
  HealthCheckReq
}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.{Source, StdIn}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object Client extends App {

  import org.rogach.scallop._

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val file = opt[String]()
    val ips = trailArg[String]()
    verify()
  }

  def createStub(port: Int) = {
    val builder =
      ManagedChannelBuilder.forAddress("localhost", port)

    builder.usePlaintext()
    val channel = builder.build()

    GeoServiceGrpc.stub(channel)
  }

  val q = 2
  val initialPort = 50_003

  val stubs = (initialPort until initialPort + q).map(createStub).toList

//  stubs.head.getLocationByIp(GetLocationByIpRequest("8.8.8.8")).onComplete {
//    case Success(GetLocationByIpReply(country, state, _)) =>
//      println(s"ip: 8.8.8.8 -> $country, $state")
//    case Failure(exception) =>
//      System.err.println("Failed ip 8.8.8.8")
//  }

  StdIn.readLine()

//  val conf = new Conf(args)

//  val ipList = conf.file
//    .map { path =>
//      val source = Source.fromFile(path)
//      val ips = source.getLines()
//      source.close()
//      ips.toList
//    }
//    .getOrElse {
//      if (!conf.ips.isSupplied) throw new RuntimeException("No ips supplied")
//      conf.ips.map(_.split(',').toList).getOrElse(List())
//    }

  val ipList = List(
    "8.8.8.8",
    "88.8.8.8",
    "8.86.8.8",
    "8.86.8.77",
    "8.86.83.8",
    "8.86.83.8",
    "8.8.83.8",
    "8.86.84.8",
    "8.86.43.8",
    "8.8.8.8",
    "8.86.48.8"
  )

  val balancer = new Balancer(stubs)

  ipList.foreach { ip =>
    balancer.run(_.getLocationByIp(GetLocationByIpRequest(ip))) {
      case Success(GetLocationByIpReply(country, state, _)) =>
        println(s"ip: $ip -> country: $country and state: $state")
      case _ => System.err.println(s"Failed to get ip: $ip")
    }
  }

  StdIn.readLine()
}

class Balancer(stubs: List[GeoServiceStub]) {

  private val healthyStubs = mutable.Set[Int]()
  private val workingStubs = mutable.Set[Int]()

  check()

  private def check(): Unit = {
    stubs.zipWithIndex.foreach { case (s, i) =>
      val future = s.healthCheck(HealthCheckReq())
      future.onComplete {
        case Success(_) => healthyStubs.add(i)
        case _          => healthyStubs.remove(i)
      }
      Await.ready(future, 500 milliseconds)
    }
  }

  @tailrec
  final def run[T](
      f: GeoServiceStub => Future[T]
  )(res: Try[T] => Unit): Unit = {

    if (healthyStubs.isEmpty) {
      System.err.println("ERROR :: All servers are down")
      System.exit(1)
    }

    val available = stubs.zipWithIndex
      .find { case (_, i) =>
        healthyStubs.contains(i) && !workingStubs.contains(i)
      }

    if (available.isEmpty) {
      check()
      run(f)(res)

    } else {
      val (stub, index) = available.get
      workingStubs add index
      val future = f(stub)
      future.onComplete(res)
      future.onComplete { _ =>
        workingStubs remove index
      }
    }
  }

}
