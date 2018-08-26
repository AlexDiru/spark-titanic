import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkApplication extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark Titanic")
    .getOrCreate()

  val rawSchema = StructType(Array(
    StructField("PassengerId", StringType, true),
    StructField("Survived", IntegerType, true),
    StructField("Pclass", IntegerType, true),
    StructField("Name", StringType, true),
    StructField("Sex", StringType, true),
    StructField("Age", FloatType, true),
    StructField("SibSp", IntegerType, true),
    StructField("Parch", IntegerType, true),
    StructField("Ticket", StringType, true),
    StructField("Fare", DoubleType, true),
    StructField("Cabin", StringType, true),
    StructField("Embarked", StringType, true)
  ))

  val df = spark
    .read
    .format("csv")
    .option("header", "true")
    .schema(rawSchema)
    .load(getClass.getResource("train.csv").getPath)

  val extractSurname : (Column) => (Column) = (name) => {
    val surnameDirty = regexp_extract(name, "^(.+?),", 0)
    val surnameClean = regexp_replace(surnameDirty, ",", "")
    trim(surnameClean)
  }

  val extractFirstName : (Column) => (Column) = (name) => {
    val firstNameDirty = regexp_extract(name, "[^,]*$", 0)
    val firstNameClean = regexp_replace(firstNameDirty, ",", "")
    trim(firstNameClean)
  }

  val convertEmbarked : (Column) => (Column) = (embarked) => {
    when(embarked === "S", 0)
      .when(embarked === "C", 1)
      .when(embarked === "Q", 2)
      .otherwise(null)
  }

  val transformedDf = df
    .withColumn("IsMale", col("Sex") === "male")
    .withColumn("FamilySize", col("SibSp") + col("Parch"))
    .withColumn("Embarked", convertEmbarked(col("Embarked")))
    .withColumn("IsAlone", when(col("FamilySize") === 1, true).otherwise(false))
    .withColumn("Title", regexp_extract(col("Name"), "([A-Za-z]+)\\.", 0))
    .withColumn("Name", regexp_replace(col("Name"), "([A-Za-z]+)\\.", ""))
    .withColumn("Name", regexp_replace(col("Name"), "\"", "")) // Remove quotes and trim the name
    .withColumn("FirstName", extractFirstName(col("Name")))
    .withColumn("Surname", extractSurname(col("Name")))
    .withColumn("Age", round(col("Age"))) // Stupidly the age is a float for a couple of rows so we round it to an integer
    .drop("Sex")
    .drop("Name")

  transformedDf.collect().foreach(println)

  transformedDf
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv("TransformedData")
}