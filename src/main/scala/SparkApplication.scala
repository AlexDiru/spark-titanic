import org.apache.spark.sql.SparkSession

object SparkApplication extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark Titanic")
    .getOrCreate()

  val df = spark.read.
    format("csv").
    option("header", "true").
    load(getClass.getResource("train.csv").getPath)
  
  println(df)
}