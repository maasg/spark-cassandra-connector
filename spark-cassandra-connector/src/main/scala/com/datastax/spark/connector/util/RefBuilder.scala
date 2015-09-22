package com.datastax.spark.connector.util

case class ConfigParameter (
  val name:String,
  val section: String,
  val default:Option[Any],
  val description:String )

object RefBuilder {

  val Header = "# Configuration Reference\n\n\n"
  val TableHeader = "Property Name | Default | Description"
  val Footer = "\n"
  
  val allConfigs = ConfigCheck.validStaticProperties
  
  def getMarkDown(): String = {
    val configBySection = allConfigs.groupBy( x => x.section)
    val sections = configBySection.keys.toSeq.sorted
    val markdown = for (section <- sections) yield {
      val parameters = configBySection.get(section).get
      val paramTable = parameters.map { case parameter: ConfigParameter =>
        val default = parameter.default match {
          case Some(defaultValue) => defaultValue
          case None => None
        }
        s"${parameter.name} | $default | ${parameter.description.replace("\n", " ")}"
      }.mkString("\n")

      s"""## $section
           |$TableHeader
           |--- | --- | ---
           |$paramTable""".stripMargin
    }
    Header + markdown.mkString("\n\n") + Footer
  }
}
