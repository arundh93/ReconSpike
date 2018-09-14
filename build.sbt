name := "recon-spike"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.15.0",
  "org.apache.hbase" % "hbase-client" % "1.1.1",
  "org.apache.hbase" % "hbase-common" % "1.1.1",
  "org.apache.hbase" % "hbase-server" % "1.1.1",
  "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"
)