name := "geotools-backend"

libraryDependencies ++= Seq(
  "org.apache.spark"            %% "spark-core"            % "3.4.0" % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-s3"         % Version.geotrellis % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % Version.geotrellis % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-geotools"   % Version.geotrellis % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-shapefile"  % Version.geotrellis % "provided",
  "de.javakaffee" % "kryo-serializers" % "0.38" exclude("com.esotericsoftware", "kryo")
)

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case x if x.startsWith("META-INF/services") => MergeStrategy.concat
  case _ => MergeStrategy.first
}

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)
