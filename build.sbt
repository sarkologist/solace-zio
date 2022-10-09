name := "SolaceZIO" 
version := "0.1.0" 
scalaVersion := "2.13.9"

libraryDependencies += "com.solacesystems" % "sol-jcsmp" % "10.4.0"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"
libraryDependencies += "dev.zio" %% "zio" % "1.0.0-RC17"

// implementation("dev.zio:zio_$scalaMajorVersion:1.0.0-RC17!!")
//    implementation("dev.zio:zio-streams_$scalaMajorVersion:1.0.0-RC17!!")