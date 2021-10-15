
import org.apache.sedona.core.formatMapper.GeoJsonReader
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, explode, from_unixtime, round, split, sum, when}
import org.apache.spark.sql.types._

object milano_analysis {

  def main(args: Array[String]): Unit = {

    // Declaration of global variables
    val start_of_day = "09:00:00"
    val end_of_day = "17:00:00"
    val start_of_lunch = "13:00:00"
    val end_of_lunch = "15:00:00"
    val interval_delta = 600000 //ms
    val chosen_pollutant = "Total Nitrogen" //pollution criterion
    val telecom_path = "/user/maaraksenov/milan_task/datasets/TELECOMMUNICATIONS_MI"

    // Starting spark session
    val spark = SparkSession
      .builder()
      .appName("Milano pollution analysis")
      .config("spark.master", "yarn")
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)

    // Reading and transforming Milano grid-map
    var milano_grid: DataFrame = spark.read
      .format("json")
      .load("/user/maaraksenov/milan_task/datasets/MI_GRID.geojson")
    milano_grid = milano_grid.withColumn("feature", explode(col("features")))
      .select(col("feature.type"), col("feature.properties"), col("feature.geometry"))
    milano_grid.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("/user/maaraksenov/milan_task/datasets/milano_grid.json")
    val milano_grid_RDD = GeoJsonReader.readToGeometryRDD(spark.sparkContext, "/user/maaraksenov/milan_task/datasets/milano_grid.json", true, false)
    milano_grid = Adapter.toDf(milano_grid_RDD, spark)
      .withColumnRenamed("geometry", "polygon")
      .withColumn("cell_id", col("cellId").cast(ShortType))
      .select(col("cell_id"), col("polygon"))

    // STG_1,2: activity
    var activity = spark.read
      .text(telecom_path)
    activity = activity.select(
      split(col("value"),"\\s+").getItem(0).as("cell_id"),
      split(col("value"),"\\s+").getItem(1).as("time_interval"),
      split(col("value"),"\\s+").getItem(3).as("sms_in"),
      split(col("value"),"\\s+").getItem(4).as("sms_out"),
      split(col("value"),"\\s+").getItem(5).as("call_in"),
      split(col("value"),"\\s+").getItem(6).as("call_out"),
      split(col("value"),"\\s+").getItem(7).as("internet")
    )
      .withColumn("cell_id", col("cell_id").cast(ShortType))
      .withColumn("time_interval", col("time_interval").cast(LongType))
      .withColumn("sms_in", col("sms_in").cast(DoubleType))
      .withColumn("sms_out", col("sms_out").cast(DoubleType))
      .withColumn("call_in", col("call_in").cast(DoubleType))
      .withColumn("call_out", col("call_out").cast(DoubleType))
      .withColumn("internet", col("internet").cast(DoubleType))
      .na.fill(0)
      .withColumn("amount",
        col("sms_in")
          + col("sms_out")
          + col("call_in")
          + col("call_out")
          + col("internet")
      )
      .groupBy(col("cell_id"), col("time_interval"))
      .agg(sum("amount").as("amount"))
      .withColumn("start_time", from_unixtime(col("time_interval") / 1000))
      .withColumn("end_time", from_unixtime((col("time_interval") + interval_delta) / 1000))
      .select(
        col("cell_id"),
        split(col("start_time")," ").getItem(0).as("date"),
        split(col("start_time")," ").getItem(1).as("start_time"),
        split(col("end_time")," ").getItem(1).as("end_time"),
        col("amount")
      )
      .filter(
        (col("start_time") < col("end_time")) &&
          (
            ((col("start_time") >= start_of_day) && (col("end_time") <= start_of_lunch)) ||
              ((col("start_time") >= end_of_lunch) && (col("end_time") <= end_of_day))
            )
      )
      .groupBy(col("cell_id"), col("date"))
      .agg(sum("amount").as("amount"))
      .groupBy(col("cell_id"))
      .agg(avg("amount").as("amount"))
      .withColumn("amount", round(col("amount"), 2).cast(FloatType))
    val avg_activity_df = activity.select(avg(col("amount")))
    val average_activity = avg_activity_df.first().getDouble(0)

    activity = activity.withColumn("cell_type", when(col("amount") <= average_activity,"living")
      .otherwise("work"))

    // STG_3: pollution
    val legend_schema = new StructType()
      .add("sensor_id", IntegerType, nullable = true)
      .add("sensor_street_name", StringType, nullable = true)
      .add("sensor_lat", DoubleType, nullable = true)
      .add("sensor_long", DoubleType, nullable = true)
      .add("sensor_type", StringType, nullable = true)
      .add("uom", StringType, nullable = true)
      .add("time_instant_format", StringType, nullable = true)
    val legend = spark.read
      .format("csv")
      .schema(legend_schema)
      .option("encoding", "windows-1252")
      .load("/user/maaraksenov/milan_task/datasets/MI_POLLUTION/LEGEND.csv")
    val pollution_schema = new StructType()
      .add("sensor_id", IntegerType, nullable = true)
      .add("time_instant", StringType, nullable = true)
      .add("measurement", FloatType, nullable = true)
    var pollution: DataFrame = spark.read
      .format("csv")
      .schema(pollution_schema)
      .load("/user/maaraksenov/milan_task/datasets/MI_POLLUTION/POLLUTION_MI")
    pollution = pollution.groupBy(col("sensor_id"))
      .agg(avg("measurement").as("measurement"))
      .withColumn("amount", round(col("measurement"), 2))
      .withColumn("amount", col("amount").cast(FloatType))
      .join(legend.withColumnRenamed("sensor_id", "sensor_id2"),
        col("sensor_id") === col("sensor_id2"),"inner")
      .select(
        col("sensor_id"),
        col("sensor_lat"),
        col("sensor_long"),
        col("sensor_type"),
        col("uom"),
        col("amount")
      )
    pollution.createOrReplaceTempView("pollution")
    milano_grid.createOrReplaceTempView("grid")
    pollution = spark.sql(
      """
        |SELECT cell_id, sensor_type AS pollutant, amount, uom
        |FROM (
        |       SELECT sensor_id, ST_Point(sensor_long, sensor_lat) AS point, sensor_type, uom, amount
        |       FROM pollution
        |     ) AS point_pollution
        |JOIN grid
        | ON ST_Within(point_pollution.point, grid.polygon)
        |ORDER BY cell_id, pollutant
        |""".stripMargin)

    // STG_4.1
    var cell_pollution: DataFrame = pollution.filter(col("pollutant") === chosen_pollutant)
      .groupBy(col("cell_id"), col("pollutant"), col("uom"))
      .agg(
        avg("amount").as("amount"),
        count("pollutant").as("sensors_num")
      )
    pollution.createOrReplaceTempView("pollution")
    cell_pollution.createOrReplaceTempView("cell_pollution")
    cell_pollution = spark.sql(
      """
        |SELECT cell_pollution.cell_id, pollutant, amount, uom, sensors_num, sensors_total
        |FROM cell_pollution
        |JOIN (
        |       SELECT cell_id, COUNT(*) AS sensors_total
        |       FROM pollution
        |       GROUP BY cell_id
        |     ) AS total
        | ON cell_pollution.cell_id = total.cell_id
        |""".stripMargin)
    cell_pollution = cell_pollution.withColumn("amount", round(col("amount"), 2))
      .withColumn("amount", col("amount").cast(FloatType))
      .withColumn("sensors_num", col("sensors_num").cast(ShortType))
      .withColumn("sensors_total", col("sensors_total").cast(ShortType))

    //STG_4.2
    cell_pollution = cell_pollution.withColumnRenamed("amount", "pollution")
      .join(activity.withColumnRenamed("cell_id", "cell_id2")
        .withColumnRenamed("amount", "activity"),
        col("cell_id") === col("cell_id2"),"inner")
      .select(
        col("cell_id"),
        col("pollutant"),
        col("pollution"),
        col("uom"),
        col("sensors_num"),
        col("sensors_total"),
        col("activity"),
        col("cell_type")
      )

    // Exporting results
    avg_activity_df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/user/maaraksenov/milan_task/results/average_activity.csv")
    cell_pollution.orderBy(col("pollution"))
      .limit(5)
      .write
      .option("header", value = true)
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/user/maaraksenov/milan_task/results/top5_clear.csv")
    cell_pollution.orderBy(col("pollution").desc)
      .limit(5)
      .write
      .option("header", value = true)
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/user/maaraksenov/milan_task/results/top5_polluted.csv")
    activity.join(cell_pollution.withColumnRenamed("cell_id", "cell_id2"),
      col("cell_id") === col("cell_id2"),"left")
      .filter(col("cell_id2").isNull)
      .select(col("cell_id"))
      .repartition(1)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/user/maaraksenov/milan_task/results/non-defined_cells.csv")

    // Printing result table
    cell_pollution.show(false)

  }
}