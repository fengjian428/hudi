/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import  org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{COW_TABLE_TYPE_OPT_VAL, HIVE_DATABASE, HIVE_SYNC_ENABLED, HIVE_SYNC_MODE, INSERT_OPERATION_OPT_VAL, OPERATION, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE, KEYGENERATOR_CLASS_NAME}
import org.apache.spark.sql.SaveMode
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.common.config.HoodieMetadataConfig


/**
 * Test suite for TableSchemaResolver with SparkSqlWriter.
 */
@Tag("functional")
class TestBootStrap {
  var spark: SparkSession = _
  var sqlContext: SQLContext = _
  var sc: SparkContext = _
  var tempPath: java.nio.file.Path = _
  var tempBootStrapPath: java.nio.file.Path = _
  var hoodieFooTableName = "hoodie_foo_tbl"
  var tempBasePath: String = _
  var commonTableModifier: Map[String, String] = Map()

  case class StringLongTest(uuid: String, ts: Long)

  /**
   * Setup method running before each test.
   */
  @BeforeEach
  def setUp(): Unit = {
    initSparkContext()
    tempPath = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    tempBootStrapPath = java.nio.file.Files.createTempDirectory("hoodie_test_bootstrap")
    tempBasePath = tempPath.toAbsolutePath.toString
    commonTableModifier = getCommonParams(tempPath, hoodieFooTableName, HoodieTableType.COPY_ON_WRITE.name())
  }

  /**
   * Tear down method running after each test.
   */
  @AfterEach
  def tearDown(): Unit = {
    cleanupSparkContexts()
    FileUtils.deleteDirectory(tempPath.toFile)
    FileUtils.deleteDirectory(tempBootStrapPath.toFile)
  }

  @Test
  def bootstrapBatch():Unit = {
    List.range(0,49).foreach( x => {
      val voucherShardDF = spark.read.parquet("hdfs://R2/projects/data_vite/hdfs/ingestion/rti/.delta_streamer_bootstrap_tmp/live/shopee_voucher_wallet_v3_db/voucher_wallet_v3_tab/bootstrap_start=1665720748832/*0" + x + "/*.parquet")
      voucherShardDF.printSchema
      voucherShardDF.coalesce(2)
      voucherShardDF.createOrReplaceTempView("hudi_table")
      val df_bucket = spark.sql("""
                SELECT *,(abs(hash(id)) % 16) as bucket_id
                FROM
                    hudi_table
        """)
      df_bucket.write.format("hudi").option(HoodieWriteConfig.TABLE_NAME, "voucher_wallet_v3_tab_test").
        option(OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL).
        option(RECORDKEY_FIELD.key, "id,_event.database,_event.table").
        option(PARTITIONPATH_FIELD.key, "region,bucket_id").
        option("hoodie.datasource.hive_sync.mode","HMS").
        option("hoodie.datasource.hive_sync.enable", "true").
        option("hoodie.datasource.hive_sync.database","dev_data_infra").
        option("hoodie.datasource.write.hive_style_partitioning", "true").
        option("hoodie.bulkinsert.shuffle.parallelism", "1024").
        option(KEYGENERATOR_CLASS_NAME.key, "org.apache.hudi.keygen.ComplexKeyGenerator").
        option("hoodie.parquet.max.file.size","536870912").option("hoodie.parquet.block.size","536870912").
        option("hoodie.bulkinsert.sort.mode", "GLOBAL_SORT").option("hoodie.copyonwrite.record.size.estimate","256").
        option("hoodie.combine.before.insert", "false").option("hoodie.sql.insert.mode", "non-strict").
        mode(SaveMode.Append).save("hdfs://R2/projects/data_vite/test/voucher_wallet_v3_tab_test1")
    })
  }
  @Test
  def bootstrap():Unit = {
    FileSystem.get( sc.hadoopConfiguration ).listStatus( new Path("hdfs://R2/projects/data_vite/hdfs/ingestion/rti/.delta_streamer_bootstrap_tmp/live/shopee_voucher_wallet_v3_db/voucher_wallet_v3_tab/bootstrap_start=1665720748832/"))
      .foreach( x => {
        println(x.getPath)
        val voucherDF = spark.read.parquet(x.getPath + "/*.parquet")
        voucherDF.printSchema
        voucherDF.coalesce(2)
        val df_bucket = spark.sql("""
                SELECT *,(abs(hash(id)) % 16) as bucket_id
                FROM
                    hudi_table
        """)
        df_bucket.write.format("hudi").option(HoodieWriteConfig.TABLE_NAME, "voucher_wallet_v3_tab_test")
          .option(OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id,_event.database,_event.table")
          .option(PARTITIONPATH_FIELD.key, "region")
          .option("hoodie.datasource.hive_sync.mode","HMS")
          .option("hoodie.datasource.hive_sync.enable", "true")
          .option("hoodie.datasource.hive_sync.database","dev_data_infra")
          .option("hoodie.datasource.write.hive_style_partitioning", "true")
          .option("hoodie.bulkinsert.shuffle.parallelism", "256")
          .option(KEYGENERATOR_CLASS_NAME.key, "org.apache.hudi.keygen.ComplexKeyGenerator")
          .option("hoodie.parquet.max.file.size","536870912").option("hoodie.parquet.block.size","536870912")
          .option("hoodie.bulkinsert.sort.mode", "PARTITION_SORT").option("hoodie.copyonwrite.record.size.estimate","256")
          .option("hoodie.combine.before.insert", "false").option("hoodie.sql.insert.mode", "non-strict")
          .mode(SaveMode.Append).save("hdfs://R2/projects/data_vite/test/voucher_wallet_v3_tab_test1")
      })
  }

  @Test
  def bucket_bootstrap():Unit = {

    val voucherShardDF = spark.read.parquet("hdfs://R2/projects/data_vite/hdfs/ingestion/rti/.delta_streamer_bootstrap_tmp/live/shopee_voucher_wallet_v3_db/voucher_wallet_v3_tab/bootstrap_start=1665720748832/*/*.parquet")
    voucherShardDF.printSchema
    voucherShardDF.createOrReplaceTempView("hudi_table")

    voucherShardDF.write.format("hudi").option(HoodieWriteConfig.TABLE_NAME, "voucher_wallet_v3_tab_test").
      option(OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL).
      option(RECORDKEY_FIELD.key, "id,_event.database,_event.table").
      option(PARTITIONPATH_FIELD.key, "region").
      option("hoodie.datasource.hive_sync.mode", "HMS").
      option("hoodie.datasource.hive_sync.enable", "true").
      option("hoodie.datasource.hive_sync.database", "dev_data_infra").
      option("hoodie.datasource.write.hive_style_partitioning", "true").
      option("hoodie.bulkinsert.shuffle.parallelism", "512").
      option("hoodie.index.type", "BUCKET").
      option("hoodie.storage.layout.type", "BUCKET").
      option("hoodie.bucket.index.num.buckets", "1000").
      option("hoodie.storage.layout.partitioner.class", "org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner").
      option(KEYGENERATOR_CLASS_NAME.key, "org.apache.hudi.keygen.ComplexKeyGenerator").
      option("hoodie.combine.before.insert", "false").option("hoodie.sql.insert.mode", "non-strict").
      mode(SaveMode.Append).save("hdfs://R2/projects/data_vite/test/voucher_wallet_v3_tab_test1")
  }





    val df_bucket = spark.sql("""
                SELECT *,(abs(hash(id)) % 16) as bucket_id
                FROM
                    hudi_table
""")





    for partition in [list of partitions in source table] {
      val inputDF = spark.read.format("any_input_format").load("partition_path")
  }

  /**
   * Utility method for initializing the spark context.
   */
  def initSparkContext(): Unit = {
    spark = SparkSession.builder()
      .appName(hoodieFooTableName)
      .master("local[2]")
      .withExtensions(new HoodieSparkSessionExtension)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sqlContext = spark.sqlContext
  }

  /**
   * Utility method for cleaning up spark resources.
   */
  def cleanupSparkContexts(): Unit = {
    if (sqlContext != null) {
      sqlContext.clearCache();
      sqlContext = null;
    }
    if (sc != null) {
      sc.stop()
      sc = null
    }
    if (spark != null) {
      spark.close()
    }
  }

  /**
   * Utility method for creating common params for writer.
   *
   * @param path               Path for hoodie table
   * @param hoodieFooTableName Name of hoodie table
   * @param tableType          Type of table
   * @return Map of common params
   */
  def getCommonParams(path: java.nio.file.Path, hoodieFooTableName: String, tableType: String): Map[String, String] = {
    Map("path" -> path.toAbsolutePath.toString,
      HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.SimpleKeyGenerator")
  }

  /**
   * Utility method for converting list of Row to list of Seq.
   *
   * @param inputList list of Row
   * @return list of Seq
   */
  def convertRowListToSeq(inputList: java.util.List[Row]): Seq[Row] =
    JavaConverters.asScalaIteratorConverter(inputList.iterator).asScala.toSeq

  @Test
  def testTableSchemaResolverInMetadataTable(): Unit = {
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    //create a new table
    val tableName = hoodieFooTableName
    val fooTableModifier = Map("path" -> tempPath.toAbsolutePath.toString,
      HoodieWriteConfig.TBL_NAME.key -> tableName,
      "hoodie.avro.schema" -> schema.toString(),
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.SimpleKeyGenerator",
      "hoodie.metadata.compact.max.delta.commits" -> "2",
      HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key -> "true"
    )

    // generate the inserts
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(10)
    val recordsSeq = convertRowListToSeq(records)
    val df1 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, fooTableModifier, df1)

    // do update
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df1)

    val metadataTablePath = tempPath.toAbsolutePath.toString + "/.hoodie/metadata"
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(metadataTablePath)
      .setConf(spark.sessionState.newHadoopConf())
      .build()

    // Delete latest metadata table deltacommit
    // Get schema from metadata table hfile format base file.
    val latestInstant = metaClient.getActiveTimeline.getCommitsTimeline.getReverseOrderedInstants.findFirst()
    val path = new Path(metadataTablePath + "/.hoodie", latestInstant.get().getFileName)
    val fs = path.getFileSystem(new Configuration())
    fs.delete(path, false)
    schemaValuationBasedOnDataFile(metaClient, HoodieMetadataRecord.getClassSchema.toString())
  }

  @ParameterizedTest
  @CsvSource(Array("COPY_ON_WRITE,parquet", "COPY_ON_WRITE,orc", "COPY_ON_WRITE,hfile",
    "MERGE_ON_READ,parquet", "MERGE_ON_READ,orc", "MERGE_ON_READ,hfile"))
  def testTableSchemaResolver(tableType: String, baseFileFormat: String): Unit = {
    val schema = DataSourceTestUtils.getStructTypeExampleSchema

    //create a new table
    val tableName = hoodieFooTableName
    val fooTableModifier = Map("path" -> tempPath.toAbsolutePath.toString,
      HoodieWriteConfig.BASE_FILE_FORMAT.key -> baseFileFormat,
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      HoodieWriteConfig.TBL_NAME.key -> tableName,
      "hoodie.avro.schema" -> schema.toString(),
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.SimpleKeyGenerator",
      HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key -> "true"
    )

    // generate the inserts
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(10)
    val recordsSeq = convertRowListToSeq(records)
    val df1 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, fooTableModifier, df1)

    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(tempPath.toAbsolutePath.toString)
      .setConf(spark.sessionState.newHadoopConf())
      .build()

    assertTrue(new TableSchemaResolver(metaClient).hasOperationField)
    schemaValuationBasedOnDataFile(metaClient, schema.toString())
  }

  /**
   * Test and valuate schema read from data file --> getTableAvroSchemaFromDataFile
   * @param metaClient
   * @param schemaString
   */
  def schemaValuationBasedOnDataFile(metaClient: HoodieTableMetaClient, schemaString: String): Unit = {
    metaClient.reloadActiveTimeline()
    var tableSchemaResolverParsingException: Exception = null
    try {
      val schemaFromData = new TableSchemaResolver(metaClient).getTableAvroSchemaFromDataFile
      val structFromData = AvroConversionUtils.convertAvroSchemaToStructType(HoodieAvroUtils.removeMetadataFields(schemaFromData))
      val schemeDesign = new Schema.Parser().parse(schemaString)
      val structDesign = AvroConversionUtils.convertAvroSchemaToStructType(schemeDesign)
      assertEquals(structFromData, structDesign)
    } catch {
      case e: Exception => tableSchemaResolverParsingException = e;
    }
    assert(tableSchemaResolverParsingException == null)
  }
}
