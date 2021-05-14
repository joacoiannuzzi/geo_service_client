package client

import io.grpc.ManagedChannelBuilder
import service.geoService.{GeoServiceGrpc, GetLocationByIpRequest}
import service.geoService.GeoServiceGrpc.GeoServiceStub

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.StdIn

object TestClient extends App {

  def createStub(
      address: String = "localhost",
      port: Int = 50004
  ): GeoServiceStub = {
    val builder =
      ManagedChannelBuilder.forAddress(address, port)

    builder.usePlaintext()
    val channel = builder.build()

    GeoServiceGrpc.stub(channel)
  }

  val stub: GeoServiceStub = createStub()

  stub
    .getLocationByIp(GetLocationByIpRequest("8.8.8.8"))
    .onComplete { r =>
      println("Response: " + r)
    }

  StdIn.readLine()
}
