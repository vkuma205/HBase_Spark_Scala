import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{concat_ws,col}

object DfOperations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HbaseBulkLoading")
      .config("spark.sql.warehouse.dir", "gardas_vinod")
      .enableHiveSupport()
      .getOrCreate()

    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "lnx0685.ch3.dev.i.com,lnx0687.ch3.dev.i.com,lnx0688.ch3.dev.i.com")
    config.set("hbase.zookeeper.property.clientPort", "5181")

    val HiveDatabase = spark.sql("use gardas_vinod")

    val orders_df = spark.sql("SELECT * FROM test_master")
    val orders_df_regular = spark.sql("SELECT * from test_regular")

    val orders_df_concat = orders_df.withColumn("extraColumn1",concat_ws("",col("order_id"),col("order_date")))
    val orders_df_regular_concat = orders_df_regular.withColumn("extraColumn2",concat_ws("",col("column1"),col("column2")))

    orders_df_concat.createOrReplaceTempView("masterTable")
    orders_df_regular_concat.createOrReplaceTempView("regularTable")

    val table_comparision = spark.sql("SELECT extraColumn2 from regularTable WHERE NOT EXISTS(SELECT extraColumn1 FROM masterTable WHERE regularTable.column1 = masterTable.order_id)")

    table_comparision.createOrReplaceTempView("tempTable")

    val results = spark.sql("select column1, column2, column3, column4 from regularTable WHERE extraColumn2 IN (SELECT extraColumn2 from tempTable)")

    val processDataFrame = results.withColumn("RowKey",concat_ws("",col("column1"),col("column2"),col("column3")))

    def catalog = s"""{
                     |"table":{"namespace":"default", "name":"DfLoad"},
                     |"rowkey":"key",
                     |"columns":{
                     |"RowKey":{"cf":"rowkey", "col":"key", "type":"string"},
                     |"column1":{"cf":"cf2", "col":"column1", "type":"string"},
                     |"column2":{"cf":"cf2", "col":"column2", "type":"string"},
                     |"column3":{"cf":"cf2", "col":"column3", "type":"string"},
                     |"column4":{"cf":"cf2", "col":"column4", "type":"string"}
                     |}
                     |}""".stripMargin

    processDataFrame.write.options(Map(
      HBaseTableCatalog.tableCatalog -> catalog,
      HBaseTableCatalog.newTable -> "2")).mode("append")
      .format("org.apache.hadoop.hbase.spark").save()

  }
}
