name := "ThreadSparkSBT"

version := "1.0"

scalaVersion := "2.11.7"
libraryDependencies ++= {
  val liftVersion = "2.6.2"
  val liftEdition = "2.6"
  Seq(
    "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
    "org.apache.spark" %% "spark-streaming" % "1.4.1",
    "org.apache.spark" %% "spark-hive" % "1.4.1",
    "net.debasishg" %% "redisclient" % "3.0",
    "org.apache.hadoop" % "hadoop-client" % "2.6.0",
    "net.liftweb" %% "lift-mongodb-record" % liftVersion
  )
}