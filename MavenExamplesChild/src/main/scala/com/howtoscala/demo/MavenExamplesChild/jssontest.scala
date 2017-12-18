package com.howtoscala.demo.MavenExamplesChild

 import org.json4s._
 import org.json4s.jackson.JsonMethods._
 import org.json4s.jackson.Serialization.write
 import org.json4s.jackson.Serialization.read
 import java.time.ZonedDateTime
 import java.time.format.DateTimeFormatter
 import org.json4s.JsonDSL._
 //import com.typesafe.scalalogging.LazyLogging
 import java.io.File
 
object jssontest extends App{
  implicit val formats = DefaultFormats 
  case class Person(name:String, age: Int) 
  val jsValue = parse("""{"name":"john", "age": 28}""") 
  val p = jsValue.extract[Person]
  val maybeP = jsValue.extractOpt[Person]
  println(p)
  println(maybeP)
}