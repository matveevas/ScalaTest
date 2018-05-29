
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import vegas._
import vegas.sparkExt._
//import com.cloudera.sparkts.models.ARIMA

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

    //https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases
    // Loading data from a JDBC source
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/callcenterdb?user=postgres&password=123")
      .option("dbtable", "callcenter.cardcriminals")
      .option("user", "postgres")
      .option("password", "123")
      .load()

    jdbcDF.show(10)

    val cardsDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/callcenterdb?user=postgres&password=123")
      .option("dbtable", "callcenter.cards")
      .option("columnname", "id, createddatetime, addresstext")
      .option("user", "postgres")
      .option("password", "123")
      .load()

    cardsDF.select("id","createddatetime", "addresstext").show(numRows = 20)

   val criminalsDF=sparkSession.read
       .format("jdbc")
       .option("url", "jdbc:postgresql://localhost/callcenterdb?user=postgres&password=123")
       .option("dbtable",
         //"(select id, addresscode, addresstext, date_trunc('day',datetime) as datetime from callcenter.cards where id in (select cardid from callcenter.cardsufferers ) and addresstext like '%Казань%' order by id)AS t")
     "( select count(id) as count ,date_trunc('day',datetime) as datetime from callcenter.cards where id in (select cardid from callcenter.cardsufferers ) and addresstext like '%Казань%' group by date_trunc('day',datetime)) as t"  )
     .option("columnname", "count, datetime")
       .option("user", "postgres")
       .option("password", "123")
       .load()
    //val criminalsDF1= criminalsDF.groupBy("datetime").count().show(numRows = 30)

      criminalsDF.select("count","datetime").show(numRows = 150)

    val plot = Vegas("Country Pop").
      withDataFrame(criminalsDF).
      encodeX("datetime", Nom).
      encodeY("count", Quant).
      encodeSize("Horsepower", Quant).
      encodeColor("Original", Nominal).
      mark(Line)

      plot.show
    //val model= ARIMA.fitModel(1,0,1,criminalsDF)



  }
}

