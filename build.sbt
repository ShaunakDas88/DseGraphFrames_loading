name := "graphframes_loading"

version := "0.1"

scalaVersion := "2.11.8"

// Please make sure that following DSE version matches your DSE cluster version.
val dseVersion = "5.1.1"

resolvers += "DataStax Repo" at "https://datastax.artifactoryonline.com/datastax/dse/"

libraryDependencies += "com.datastax.dse" % "dse-spark-dependencies" % dseVersion % "provided"
