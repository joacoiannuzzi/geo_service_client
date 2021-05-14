package client

import io.grpc.ManagedChannelBuilder
import service.geoService.GeoServiceGrpc
import service.geoService.GeoServiceGrpc.GeoServiceStub

import java.net.InetAddress

object Util {
  def createStub(url: String): GeoServiceStub = {
    val builder =
      ManagedChannelBuilder.forTarget(url)//forAddress(InetAddress.getLocalHost.getHostAddress, port)

    builder.usePlaintext()
    val channel = builder.build()

    GeoServiceGrpc.stub(channel)
  }
}
