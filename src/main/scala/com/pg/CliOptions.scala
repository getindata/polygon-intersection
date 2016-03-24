package com.pg

class CliOptions(argsList: List[String]) extends Serializable {

  type OptionMap = Map[Symbol, Any]

  private val POLYGONS_CSV: Symbol = 'polygons_csv
  private val RESULT_TABLE: Symbol = 'result_table

  private val optionMap = parseOptions(argsList)

  val polygonsCsv = getValueString(POLYGONS_CSV)
  val resultTable = getValueString(RESULT_TABLE)

  private def getValueString(s: Symbol): String = {
    optionMap.getOrElse(s, "none").toString
  }

  def parseOptions(argsList: List[String]): OptionMap = {
    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      (list: @unchecked) match {
        case Nil => map
        case "--polygons_csv" :: value :: tail =>
          nextOption(map ++ Map(POLYGONS_CSV -> value), tail)
        case "--result_table" :: value :: tail =>
          nextOption(map ++ Map(RESULT_TABLE -> value), tail)
      }
    }
    nextOption(Map(), argsList)
  }

}
