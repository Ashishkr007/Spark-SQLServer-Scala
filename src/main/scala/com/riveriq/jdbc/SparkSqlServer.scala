package com.riveriq.jdbc
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.io.FileNotFoundException
import java.io.IOException
import java.sql.DriverManager;
import java.sql.Connection
import java.util.Properties

object SparkSqlServer extends Serializable {

  def main(args: Array[String]) = {

    val SparkApplicationName = "Spark-SqlServer-Example"

    val sparkSession = SparkSession.builder
      .appName(SparkApplicationName)
      .config("spark.sql.shuffle.partitions", "20")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.dynamicAllocation.enabled", "true") //enable dynamic allocation in spark
      .config("spark.shuffle.service.enabled", "true") //enable dynamic allocation in spark
      .enableHiveSupport()
      .getOrCreate()

    val jdbcUsername = "riveriq"
    val jdbcPassword = "riveriq123$"
    val database = "employee"
    val Driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val jdbcserver = "jdbc:sqlserver://127.0.0.1:1433"
    val tableName = "EmpDetails"
    val DestPath = "/user/riveriq/employee/EmpDetails"
    val connectionProperties = new Properties()
    connectionProperties.put("database", database)
    connectionProperties.put("user", jdbcUsername)
    connectionProperties.put("password", jdbcPassword)
    connectionProperties.put("encrypt", "false")
    connectionProperties.put("trustServerCertificate", "false")
    connectionProperties.setProperty("Driver", Driver)
    val jdbcUrl = jdbcserver + ";database=" + database + ";user=" + jdbcUsername + ";password=" + jdbcPassword
    println("jdbcUrl " + jdbcUrl)

    val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)

    try {
      if (!connection.isClosed()) {
        val df_stable = sparkSession.read.jdbc(jdbcUrl, tableName, connectionProperties)
        df_stable
          .coalesce(1)
          .write.mode("overwrite").format("csv")
          .option("delimiter", ",")
          .option("quote", "\"")
          .option("compression", "none")
          .option("quoteAll", "true")
          .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
          .save(DestPath)
      }
    } catch {
      case ex: Exception =>
        {
          println(ex)
        }
    }
    sparkSession.stop()
  }

}