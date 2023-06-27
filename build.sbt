name := "Message Queue Sender and Receiver"

version := "0.1"

scalaVersion := "2.13.10"

enablePlugins(AssemblyPlugin)

assembly / mainClass := Some("MessageQueueSendReceive")

libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.17"
)

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.4.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "3.4.0"

// For including the IBM DB2 Database JDBC driver:
//libraryDependencies += "com.ibm.db2.jcc" % "db2jcc" % "db2jcc4"
// https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc

// Un-comment this line for including the Oracle Database JDBC driver:
//libraryDependencies += "com.oracle.database.jdbc" % "ojdbc10" % "19.17.0.0"
// https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc10

// Un-comment this line for including the Microsoft SQL Server Database JDBC driver:
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "11.2.3.jre11"
// https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc

// Un-comment this line for including the Postgresql Database JDBC driver:
libraryDependencies += "org.postgresql" % "postgresql" % "42.5.1"
// https://mvnrepository.com/artifact/org.postgresql/postgresql

// Un-comment this line for including the MySQL Database JDBC driver:
// MySQL database JDBC driver:
//libraryDependencies += "com.mysql" % "mysql-connector-j" % "8.0.32"

// config file reader:
libraryDependencies += "com.typesafe" % "config" % "1.4.2"

// for testing:
coverageEnabled := true

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.15"

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused"
)

