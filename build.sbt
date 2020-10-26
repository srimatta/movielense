name := "movielense"

version := "0.1"

val scalaVersion = "2.12.0"
val sparkVersion = "3.0.1"
val typeSafeConfigVersion = "1.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % typeSafeConfigVersion

)
