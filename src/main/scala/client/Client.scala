package client

import client.BalancerUtils.getStubs
import client.Util.createStub
import client.Utils.getIpList
import io.etcd.jetcd.options.{GetOption, WatchOption}
import io.etcd.jetcd.watch.{WatchEvent, WatchResponse}
import io.etcd.jetcd.{ByteSequence, Client, KV, Watch}
import org.rogach.scallop._
import service.geoService.GeoServiceGrpc.{GeoServiceStub, stub}
import service.geoService._

import java.nio.charset.{Charset, StandardCharsets}
import java.util.concurrent.CountDownLatch
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.{Source, StdIn}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class ArgParser(arguments: Seq[String]) extends ScallopConf(arguments) {
  val file = opt[String]()
  val ips = trailArg[String](required = false)
  verify()
}

object Utils {
  def getIpList(args: ArgParser) = {
    args.file
      .map { path =>
        val source = Source.fromFile(path)
        val ips = source.getLines().toList
        source.close()
        ips
      }
      .getOrElse {
        if (!args.ips.isSupplied) {
          System.err.println("ERROR :: No ips supplied")
          sys.exit(1)
        }
        args.ips.map(_.split(',').toList).getOrElse(List())
      }
  }
}

object Client2 extends App {

  val argsParsed = new ArgParser(args)

  val ipList: List[String] = getIpList(argsParsed)

  val balancer = Balancer()

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

  StdIn.readLine()
}

case class Balancer() {

  val client: Client =
    Client.builder().endpoints("http://localhost:2379").build()

  val kvClient: KV = client.getKVClient
  val watchClient: Watch = client.getWatchClient

  val stubs = getStubs(kvClient)

  private val healthyStubs = mutable.Set[Int]()
  private val workingStubs = mutable.Set[Int]()

  var stubsMutable: ListBuffer[GeoServiceStub] = ListBuffer()
  stubsMutable ++= stubs

  checkStubs()

  private def checkStubs(): Unit = {
    val key = ByteSequence.from("service/geo/", StandardCharsets.UTF_8)
    val latch: CountDownLatch = new CountDownLatch(1)

    val listener = Watch.listener((response: WatchResponse) => {
      print("Watching for key=something")

      response.getEvents
        .stream()
        .map(event => {
          event.getEventType match {
            case WatchEvent.EventType.PUT =>
              stubsMutable += createStub(
                event.getKeyValue.getValue.toString(Charset.forName("UTF-8"))
              )
            case WatchEvent.EventType.DELETE =>
              stubsMutable -= createStub(
                event.getKeyValue.getValue.toString(Charset.forName("UTF-8"))
              )
            case WatchEvent.EventType.UNRECOGNIZED => None
          }
        })
    })

    try {
      val watchOption = WatchOption.newBuilder().withPrefix(key).build()
      val watch: Watch = watchClient
      val watcher: Watch.Watcher = watch.watch(key, watchOption, listener)
      try {
        println("before latch")
        latch.await()
        println("after latch")

      } catch {
        case e: Exception =>
          System.exit(1)
      } finally {
        if (client != null) client.close()
        if (watch != null) watch.close()
        if (watcher != null) watcher.close()
        if (kvClient != null) kvClient.close()
        if (watchClient != null) watchClient.close()
      }
    } catch {
      case e: Exception =>
        System.exit(1)
    }
  }

  def run[T](
      caller: GeoServiceStub => Future[T]
  )(responder: Try[T] => Unit): Unit = {

    def runInside(
        caller: GeoServiceStub => Future[T]
    )(responder: Try[T] => Unit)(n: Int): Unit = {

      runAux(caller) {
        case Failure(e) =>
          if (n > 5) {
            responder(Failure(e))
          } else runInside(caller)(responder)(n + 1)

        case Success(value) =>
          responder(Success(value))
      }
    }

    runInside(caller)(responder)(0)
  }

  @tailrec
  private final def runAux[T](
      caller: GeoServiceStub => Future[T]
  )(responder: Try[T] => Unit): Unit = {

    val available = stubs.zipWithIndex
      .find { case (_, i) =>
        healthyStubs.contains(i) && !workingStubs.contains(i)
      }

    available match {
      case Some((stub, index)) =>
        workingStubs add index
        val future = caller(stub)
        future.onComplete(_ => workingStubs remove index)
        future.onComplete(responder)
      case None => runAux(caller)(responder)
    }
  }
}

object BalancerUtils {

  def getStubs(kvClient: KV) = {

    val key: ByteSequence = ByteSequence.from("service/geo".getBytes())

    val keyValueMap = mutable.Map[String, String]()

    try {
      val option = GetOption.newBuilder.withRange(key).build
      val futureResponse = kvClient.get(key, option)
      val response = futureResponse.get
      response.getKvs
        .stream()
        .forEach(kv => {
          keyValueMap.put(
            kv.getKey.toString(Charset.forName("UTF-8")),
            kv.getValue.toString(Charset.forName("UTF-8"))
          )
        })
    } catch {
      case e: Exception =>
        throw new RuntimeException("Failed to retrieve any key.", e)
    }
    //  val stubs = keyValueMap.values.map(createStub).toList
    val stubs = keyValueMap.values.map(x => createStub(x)).toList
    stubs
  }
}
