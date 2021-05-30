package client

import client.BalancerUtils.getStubs
import client.EtcdUtils.getEtcdClient
import client.Util.createStub
import client.Utils.getIpList
import io.etcd.jetcd.kv.GetResponse
import io.etcd.jetcd.options.{GetOption, WatchOption}
import io.etcd.jetcd.watch.{WatchEvent, WatchResponse}
import io.etcd.jetcd.{ByteSequence, Client, KV, Watch}
import org.rogach.scallop._
import service.geoService.GeoServiceGrpc.GeoServiceStub
import service.geoService._

import java.nio.charset.{Charset, StandardCharsets}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.{Source, StdIn}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object EtcdUtils {
  private val endpoint: String = sys.env("ETCD_ENDPOINT")

  def getEtcdClient: Client = {
    println(s"endpoint $endpoint")
    Client.builder().endpoints(endpoint).build()
  }
}

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
          System.err.println("No ips supplied, using defaults")
          List(
            "8.8.8.8",
            "88.8.8.8",
            "8.86.8.8",
            "8.86.8.77",
            "8.86.83.8",
            "8.86.83.5",
            "8.8.83.8",
            "8.86.84.8",
            "8.86.43.8",
            "8.8.8.8",
            "8.86.48.8"
          )
        } else args.ips.map(_.split(',').toList).getOrElse(List())
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

  val client: Client = getEtcdClient
  val key: ByteSequence = ByteSequence.from("service/geo".getBytes())

  val kvClient: KV = client.getKVClient
  val watchClient: Watch = client.getWatchClient

  val stubs = getStubs(kvClient)

  private val workingStubs = mutable.Set[String]()

  checkStubs()

  private def checkStubs(): Unit = {
    val key = ByteSequence.from("service/geo/", StandardCharsets.UTF_8)

    val listener = Watch.listener((response: WatchResponse) => {
      println(s"response = $response")

      response.getEvents
        .stream()
        .forEach(event => {

          event.getEventType match {
            case WatchEvent.EventType.PUT =>
              stubs.put(
                event.getKeyValue.getKey.toString(Charset.forName("UTF-8")),
                createStub(
                  event.getKeyValue.getValue.toString(Charset.forName("UTF-8"))
                )
              )
            case WatchEvent.EventType.DELETE => {
              stubs.remove(
                event.getKeyValue.getKey.toString(Charset.forName("UTF-8"))
              )
              workingStubs.remove(
                event.getKeyValue.getKey.toString(Charset.forName("UTF-8"))
              )
            }
            case WatchEvent.EventType.UNRECOGNIZED => None
          }
        })

//      println(stubs)
    })

    val watchOption = WatchOption.newBuilder().withPrefix(key).build()
    val watch: Watch = watchClient
    val watcher: Watch.Watcher = watch.watch(key, watchOption, listener)

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

    val available = stubs
      .find { case (k, _) =>
        !workingStubs.contains(k)
      }

    available match {
      case Some((key, stub)) =>
        println(s"using $key")
        workingStubs add key
        val future = caller(stub)
        future.onComplete(_ => workingStubs remove key)
        future.onComplete(responder)
      case None => runAux(caller)(responder)
    }
  }
}

object BalancerUtils {
  val key: ByteSequence = ByteSequence.from("service/geo".getBytes())

  def getStubs(kvClient: KV) = {
    val keyValueMap = mutable.Map[String, String]()
    try {

      val option: GetOption = GetOption
        .newBuilder()
        .withSortField(GetOption.SortTarget.KEY)
        .withSortOrder(GetOption.SortOrder.DESCEND)
        .withPrefix(key)
        .build()

      val futureResponse = kvClient.get(key, option);

      val response: GetResponse = futureResponse.get();

      if (response.getKvs().isEmpty()) {
        println("Failed to retrieve any key.");
      }

      response.getKvs.forEach(x => {
        keyValueMap.put(
          x.getKey.toString(Charset.forName("UTF-8")),
          x.getValue.toString(Charset.forName("UTF-8"))
        )
      })

      println("Retrieved " + response.getKvs().size() + " keys.");

    } catch {
      case e: Exception => print("Failed to retrieve any key.");
    }
    keyValueMap.map { case (key, value) =>
      (key, createStub(value))
    }

  }
}
