import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

object TestScala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    conf.setAppName("Spark Hello World")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //jdbc:postgresql://localhost/test?user=fred&password=secret
    // Loading data from a JDBC source
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/callcenterdb?user=postgres&password=123")
      .option("dbtable", "callcenter.cardcriminals")
      .option("user", "postgres")
      .option("password", "123")
      .load()

    jdbcDF.show(10)


  }
}

