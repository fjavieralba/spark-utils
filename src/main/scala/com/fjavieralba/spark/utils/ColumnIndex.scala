package com.fjavieralba.spark.utils

/** Simple info about a table */
case class TableInfo(val name: String, val columns: List[String])

/**
  * An index of columns to have a quick access to what tables contain what columns
  */
class ColumnIndex(val tables: List[TableInfo]) {

  val index: Map[String, List[String]] = tables.map(buildIndexFromTableInfo).reduce(mergeIndexes)

  def getColumnTables(column: String): List[String] = {
    val normalizedCol = normalize(column)
    if (index contains normalizedCol) index(normalizedCol) else List()
  }

  def merge(otherIndex: ColumnIndex): ColumnIndex = {
    new ColumnIndex(tables ++ otherIndex.tables)
  }

  private def buildIndexFromTableInfo(tableInfo: TableInfo): Map[String, List[String]] = {
    var index: Map[String, List[String]] = Map()
    for (col <- tableInfo.columns) {
      val normalizedCol = normalize(col)
      index = index + (normalizedCol -> List(tableInfo.name))
    }
    index
  }

  private def normalize(column: String): String = column.toLowerCase


  private def mergeIndexes(m1: Map[String, List[String]], m2: Map[String, List[String]]): Map[String, List[String]] = {
    m1 ++ m2.map { case (k, v) => k -> (v ++ m1.getOrElse(k, Nil))}
  }

}
