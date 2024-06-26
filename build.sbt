name := "geo_service_client"

version := "0.1"

scalaVersion := "2.13.5"

libraryDependencies ++= Seq(
  "io.grpc" % "grpc-netty" % "1.4.0",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)

libraryDependencies += "io.etcd" % "jetcd-core" % "0.5.4"
Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)

// (optional) If you need scalapb/scalapb.proto or anything from
// google/protobuf/*.proto
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
)

libraryDependencies += "org.rogach" %% "scallop" % "4.0.2"
libraryDependencies += "io.etcd" % "jetcd-core" % "0.5.4"

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)

mainClass in Compile := Some("client.Client2")
packageName in Docker := "geo-service-client"
