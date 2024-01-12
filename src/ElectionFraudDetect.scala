import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object ElectionFraudDetect {
  val conf = new SparkConf()
    .setAppName("sample App")
    .setMaster("local")
  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._

  def main(args: Array[String]) {
    val UKdf = sqlContext.read.format("com.databricks.spark.csv").option("Header", true).option("inferSchema", true)
      .csv("C:/Users/vyshali.acharya/workspace/ElectionFraudDetection/UK2010.csv")
    UKdf.printSchema()
    
    val Russia1 = sqlContext.read.format("com.databricks.spark.csv").option("Header", true).option("inferSchema", true).csv("C:/Users/vyshali.acharya/workspace/ElectionFraudDetection/Russia2011_1of2.csv")
    val Russia2 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", true).csv("C:/Users/vyshali.acharya/workspace/ElectionFraudDetection/Russia2011_2of 2.csv")
    val RussiaDF = Russia1.union(Russia2)
    
   
    UKdf.groupBy().sum("Electorate").show()
    RussiaDF.groupBy().sum("Number of voters included in voters list").show()
    
    UKdf.groupBy().sum("Votes").show()
    RussiaDF.groupBy().sum("Number of valid ballots").show()
    
    
  }
}