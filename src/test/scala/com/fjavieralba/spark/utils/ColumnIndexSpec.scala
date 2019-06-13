package com.fjavieralba.spark.utils

import org.scalatest.FlatSpec

class ColumnIndexSpec extends FlatSpec {
  "A com.basf.spark.utils.ColumnIndex" should "be correctly built from info from a table" in {
    val tableInfo1 = TableInfo("myTable", List("c1", "c2", "c3"))
    val index = new ColumnIndex(List(tableInfo1))
    assert(index.getColumnTables("c1") contains "myTable")
  }

  "Two ColumnIndexes" should "be correctly merged" in {
    val tableInfo1 = TableInfo("myTable1", List("c1", "c2", "c3"))
    val tableInfo2 = TableInfo("myTable2", List("c1", "c2", "cX"))
    val index1 = new ColumnIndex(List(tableInfo1))
    val index2 = new ColumnIndex(List(tableInfo2))
    val merged = index1.merge(index2)
    assert(merged.getColumnTables("c1").toSet === Set("myTable1", "myTable2"))
  }

  "Two merged column indexes" should "be the same as a single column index from the same set of columns/tables" in {
    val tInfo1 = TableInfo("myTable1", List("c1", "c2", "c3"))
    val tInfo2 = TableInfo("myTable2", List("c1", "c2", "cx"))
    val index1 = new ColumnIndex(List(tInfo1))
    val index2 = new ColumnIndex(List(tInfo2))
    val merged = index1.merge(index2)
    val fromScratch = new ColumnIndex(List(tInfo1, tInfo2))
    assert(merged.index === fromScratch.index)
  }

  "columns indexes" should "be case-insensitive when looking for columns" in {
    val tInfo1 = TableInfo("myTable1", List("A", "b", "Cd"))
    val index = new ColumnIndex(List(tInfo1))
    assert(index.getColumnTables("a") === List("myTable1"))
    assert(index.getColumnTables("B") === List("myTable1"))
    assert(index.getColumnTables("cD") === List("myTable1"))
  }

  "An empty list of tables" should "be returned if a column is not found" in {
    val tInfo1 = TableInfo("myTable1", List("a", "b", "c"))
    val index = new ColumnIndex(List(tInfo1))
    assert(index.getColumnTables("X") === List())
  }
}
