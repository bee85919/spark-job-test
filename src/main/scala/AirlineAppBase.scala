import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Dataset, SparkSession}

abstract class AirlineAppBase extends App {
  def writeDsToPath[U](ds: Dataset[U], path: String): Unit = {
    ds.write.csv(path)
  }

  def createAirlineOriginDataset(spark: SparkSession, inputPath: String): Dataset[AirlineOrigin] = {
    import spark.implicits._
    spark.read.
      option("header", false).
      option("inferSchema", true).
      csv(inputPath).toDF(AirlineColumns.names: _*).as[AirlineOrigin]
  }


  def sparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SparkExampleApp")
      .set("spark.hadoop.dfs.replication", "3")
      .set("spark.master", "yarn")

    sparkConf
  }

  def createSparkSession() : SparkSession = {
    SparkSession.builder()
      .config(sparkConf())
      .getOrCreate()
  }

  def run(spark: SparkSession, ds: Dataset[Airline]): Unit

  var inputPath: String = ""
  var outputPath: String = ""
  if (args.length >= 2) {
    args.sliding(2, 2).toList.collect {
      case Array("--input", input: String) => inputPath = Option(input).getOrElse("")
      case Array("--output", output: String) => outputPath = Option(output).getOrElse("")
    }
  } else {
    System.err.println("invalid args. Guide: --input $input --output $output")
  }

  val udfConvertValueNAtoNull = udf({ value: String =>
    if (value == null || value.isEmpty || "NA".equals(value.toUpperCase())) None
    else Some(value.toInt)
  }: (String => Option[Int]))

  val spark = createSparkSession()

  import spark.implicits._

  val airlineDS = createAirlineOriginDataset(spark, inputPath).
    withColumn("date",
      concat(col("year"), lit("-"), col("month"), lit("-"), col("dayofmonth"))
        .cast(DateType)
    ).
    withColumn("year", udfConvertValueNAtoNull(col("year"))).
    withColumn("month", udfConvertValueNAtoNull(col("month"))).
    withColumn("dayofmonth", udfConvertValueNAtoNull(col("dayofmonth"))).
    withColumn("dayofweek", udfConvertValueNAtoNull(col("dayofweek"))).
    withColumn("deptime", udfConvertValueNAtoNull(col("deptime"))).
    withColumn("crsdeptime", udfConvertValueNAtoNull(col("crsdeptime"))).
    withColumn("arrtime", udfConvertValueNAtoNull(col("arrtime"))).
    withColumn("crsarrtime", udfConvertValueNAtoNull(col("crsarrtime"))).
    withColumn("flightnum", udfConvertValueNAtoNull(col("flightnum"))).
    withColumn("actualelapsedtime", udfConvertValueNAtoNull(col("actualelapsedtime"))).
    withColumn("crselapsedtime", udfConvertValueNAtoNull(col("crselapsedtime"))).
    withColumn("airtime", udfConvertValueNAtoNull(col("airtime"))).
    withColumn("arrdelay", udfConvertValueNAtoNull(col("arrdelay"))).
    withColumn("depdelay", udfConvertValueNAtoNull(col("depdelay"))).
    withColumn("distance", udfConvertValueNAtoNull(col("distance"))).
    withColumn("taxiin", udfConvertValueNAtoNull(col("taxiin"))).
    withColumn("taxiout", udfConvertValueNAtoNull(col("taxiout"))).
    withColumn("cancelled", udfConvertValueNAtoNull(col("cancelled"))).
    withColumn("diverted", udfConvertValueNAtoNull(col("diverted"))).
    withColumn("carrierdelay", udfConvertValueNAtoNull(col("carrierdelay"))).
    withColumn("weatherdelay", udfConvertValueNAtoNull(col("weatherdelay"))).
    withColumn("nasdelay", udfConvertValueNAtoNull(col("nasdelay"))).
    withColumn("securitydelay", udfConvertValueNAtoNull(col("securitydelay"))).
    withColumn("arr_delayed", when(col("arrdelay") > 0, true).otherwise(false)).
    withColumn("dep_delayed", when(col("arrdelay") > 0, true).otherwise(false)).
    as[Airline]

  run(spark, airlineDS)



}