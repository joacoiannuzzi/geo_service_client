package client

import client.Util.createStub
import io.etcd.jetcd.options.WatchOption
import io.etcd.jetcd.watch.{WatchEvent, WatchResponse}
import io.etcd.jetcd.{Client, Watch}
import org.rogach.scallop._
import service.geoService.GeoServiceGrpc.GeoServiceStub
import service.geoService._
import io.etcd.jetcd.ByteSequence

import java.nio.charset.{Charset, StandardCharsets}
import java.util.concurrent.CountDownLatch
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}



object GeoClient extends App {

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

  val client: Client = Client.builder().endpoints("http://localhost:2379").build()
  val kvClient = client.getKVClient
  val key: ByteSequence = ByteSequence.from("service/geo".getBytes())

  import io.etcd.jetcd.options.GetOption
  val keyValueMap = mutable.Map[String, String]()

  try {
    val option = GetOption.newBuilder.withRange(key).build
    val futureResponse = kvClient.get(key, option)
    val response = futureResponse.get
    response.getKvs.stream().map(kv =>{
      keyValueMap.put(kv.getKey.toString(Charset.forName("UTF-8")), kv.getValue.toString(Charset.forName("UTF-8")))
    })
  } catch {
    case e: Exception =>
      throw new Nothing("Failed to retrieve any key.", e)
  }
  val stubs = keyValueMap.values.map(createStub).toList

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

  val client = Client.builder().endpoints("http://localhost:2379").build()
  val kvclient = client.getKVClient
  val watchClient = client.getWatchClient
  private val healthyStubs = mutable.Set[Int]()
  private val workingStubs = mutable.Set[Int]()
  var stubsMutable: ListBuffer[GeoServiceStub] = ListBuffer[GeoServiceStub]
  stubsMutable ++= stubs
  checkStubs()

  private var lastCheck = System.currentTimeMillis()
  private var timeSinceServersDown = 0L
  private var calls = 0
  private var responses = 0

  def await(): Unit = {
    while (calls != responses) print("") // without the print it doesn't work
  }

  private def checkStubs(): Unit = {
    val key = ByteSequence.from("service/geo/", StandardCharsets.UTF_8)
    val latch: CountDownLatch = new CountDownLatch(1)

    val listener = Watch.listener((response: WatchResponse) => {
      print("Watching for key=something")

      response.getEvents.stream().map(event => {
        event.getEventType match {
          case WatchEvent.EventType.PUT => stubsMutable += createStub(event.getKeyValue.getValue.toString(Charset.forName("UTF-8")))
          case WatchEvent.EventType.DELETE => stubsMutable -= createStub(event.getKeyValue.getValue.toString(Charset.forName("UTF-8")))
        }
      })
    })
    try {
      val watchOption = WatchOption.newBuilder().withPrefix(key).build()
      val watch: Watch = watchClient
      val watcher: Watch.Watcher = watch.watch(key, watchOption, listener)
      try latch.await()
      catch {
        case e: Exception =>
          System.exit(1)
      } finally {
        if (client != null) client.close()
        if (watch != null) watch.close()
        if (watcher != null) watcher.close()
        if (kvclient != null) kvclient.close()
        if (watchClient != null) watchClient.close()
      }
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
        checkStubs()
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
