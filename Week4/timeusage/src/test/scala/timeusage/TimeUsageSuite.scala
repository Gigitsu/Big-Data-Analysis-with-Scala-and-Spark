package timeusage

import org.apache.spark.sql.{DataFrame, Dataset}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {
  lazy val timeUsage: TimeUsage.type = TimeUsage

  lazy val (columns, df) = timeUsage.read("/timeusage/atussum-10000.csv")
  lazy val (primaryNeedsColumns, workColumns, otherColumns) = timeUsage.classifiedColumns(columns)

  lazy val summaryDf: DataFrame = timeUsage.timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, df)
  lazy val finalDf: DataFrame = timeUsage.timeUsageGrouped(summaryDf)

  lazy val sqlDf: DataFrame = timeUsage.timeUsageGroupedSql(summaryDf)
  lazy val summaryDs: Dataset[TimeUsageRow] = timeUsage.timeUsageSummaryTyped(summaryDf)
  lazy val finalDs: Dataset[TimeUsageRow] = timeUsage.timeUsageGroupedTyped(summaryDs)

  test("classifiedColumns") {
    val pc = primaryNeedsColumns.map(_.toString)
    val wc = workColumns.map(_.toString)
    val oc = otherColumns.map(_.toString)

    assert(pc.contains("t010199"))
    assert(pc.contains("t030501"))
    assert(pc.contains("t110101"))
    assert(pc.contains("t180382"))
    assert(wc.contains("t050103"))
    assert(wc.contains("t180589"))
    assert(oc.contains("t020101"))
    assert(oc.contains("t180699"))

    assert(!pc.contains("tucaseid"))
    assert(!wc.contains("tucaseid"))
    assert(!oc.contains("tucaseid"))
  }

  test("timeUsageSummary") {
    assert(summaryDf.columns.length === 6)
    assert(summaryDf.count === 6872)
    summaryDf.show()
  }


  test("timeUsageGrouped") {
    assert(finalDf.count === 2 * 2 * 3)
    assert(finalDf.head.getDouble(3) === 12.3)
    finalDf.show()
  }

  test("timeUsageGroupedSql") {
    assert(sqlDf.count === 2 * 2 * 3)
    assert(sqlDf.head.getDouble(3) === 12.3)
    sqlDf.show()
  }

  test("timeUsageSummaryTyped") {
    assert(summaryDs.head.getClass.getName === "timeusage.TimeUsageRow")
    assert(summaryDs.head.other === 8.75)
    assert(summaryDs.count === 6872)
    summaryDs.show()
  }

  test("timeUsageGroupedTyped") {
    assert(finalDs.count === 2 * 2 * 3)
    assert(finalDs.head.primaryNeeds === 12.3)
    assert(finalDs.head.primaryNeeds === 12.3)
  }
}
