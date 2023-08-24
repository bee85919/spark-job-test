import org.apache.spark.sql.functions.{avg, col, stddev, sum}
import org.apache.spark.sql.{Dataset, SparkSession}

object AirlineDelayStatsApp extends AirlineAppBase {
  override def run(spark: SparkSession, ds: Dataset[Airline]): Unit = {
    val countByOrigin = ds.groupBy(col("year"), col("origin")).
      count()

    val statsByOrigin = ds.groupBy(col("year"), col("origin")).
      agg(
        sum("depdelay"),
        avg("depdelay"),
        stddev("depdelay")
      )

    val totalStatsByOrigin = statsByOrigin.join(countByOrigin, Seq("year", "origin"))

    val countByDest = ds.groupBy(col("year"), col("dest"))
      .count()

    val statsByDest = ds.groupBy(col("year"), col("dest"))
      .agg(
        sum("arrdelay"),
        avg("arrdelay"),
        stddev("arrdelay")
      )

    val totalStatsByDest = statsByDest.join(countByDest, Seq("year", "dest"))

    writeDsToPath(totalStatsByOrigin, s"$outputPath/stats_by_origin")
    writeDsToPath(totalStatsByDest, s"$outputPath/stats_by_dest")
  }
}