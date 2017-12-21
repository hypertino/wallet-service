crossScalaVersions := Seq("2.12.4", "2.11.12")

scalaVersion := crossScalaVersions.value.head

lazy val `wallet-service` = project in file(".") enablePlugins Raml2Hyperbus settings (
    name := "wallet-service",
    version := "0.5-SNAPSHOT",
    organization := "com.hypertino",
    resolvers ++= Seq(
      Resolver.sonatypeRepo("public")
    ),
    libraryDependencies ++= Seq(
      "com.hypertino" %% "hyperbus" % "0.5-SNAPSHOT",
      "com.hypertino" %% "hyperbus-t-inproc" % "0.5-SNAPSHOT" % "test",
      "com.hypertino" %% "service-control" % "0.4.1",
      "com.hypertino" %% "service-config" % "0.2.0" % "test",
      "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0" % "test",
      "com.hypertino" %% "hyperstorage" % "0.6-SNAPSHOT" % "test",
      compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
    ),
    ramlHyperbusSources := Seq(
      ramlSource(
        path = "api/wallet-service-api/wallet.raml",
        packageName = "com.hypertino.wallet.api",
        isResource = false
      ),
      ramlSource(
        path = "api/hyperstorage-service-api/hyperstorage.raml",
        packageName = "com.hypertino.user.apiref.hyperstorage",
        isResource = false
      )
    )
)
