

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
    "org.mongodb" %% "casbah" % "2.8.1",
    "commons-lang" % "commons-lang" % "2.6",
    "org.apache.hadoop" % "hadoop-client" % "2.6.0",
    "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" % "test" artifacts Artifact("javax.servlet", "jar", "jar"),
    "net.liftweb" %% "lift-mongodb-record" % liftVersion
  )
}