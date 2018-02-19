import java.util.Properties

import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{desc, row_number}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

object ProductCounter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CategoryCounter")
    val sparkContext = new SparkContext(conf)
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    SparkSession.builder.enableHiveSupport()

    val connectionProperties = new Properties()
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    connectionProperties.put("url", "jdbc:mysql://10.0.0.21:3306/snikitin")
    connectionProperties.put("user", "snikitin")

    val textFiles = sparkContext.textFile("hdfs://" + args(0))

    val splited = textFiles.map(_.split(","))

    val filtered = splited.map(x => (x(3), x(0), 1L))

    val grouped = filtered.map { case (category, product, count) => ((category, product), count) }.reduceByKey(_ + _)

    val sorted = grouped.sortBy(x => (x._1._1, x._2), ascending = false)

    val flatted = sorted.map{ case ((category, product), count) => Row(category, product, count) }

    val schema = StructType($"category".string :: $"product".string :: $"count".long :: Nil)

    val dataFrame = sparkSession.createDataFrame(flatted, schema)

    val window = Window.partitionBy("category").orderBy(desc("count"))

    val answer = dataFrame.withColumn("rank", row_number.over(window)).filter("rank <= 10").drop("rank")

    answer.write.jdbc(connectionProperties.getProperty("url"), "spark_sales_per_product", connectionProperties)
  }
}
