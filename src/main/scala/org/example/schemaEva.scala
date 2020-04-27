package org.example

import java.nio.file.{Files, Paths}

object schemaEva {

  def schemaEvaluation(tpc: String): String = tpc match {
    case "demo" => new String(Files.readAllBytes(Paths.get("/home/slave/IdeaProjects/Practice/src/main/resources/schema/sam.avsc")))
  }

}
