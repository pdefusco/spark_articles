/* ****************************************************************************
* (C) Cloudera, Inc. 2020-2025
*  All rights reserved.
*
*  Applicable Open Source License: GNU Affero General Public License v3.0
*
*  NOTE: Cloudera open source products are modular software products
*  made up of hundreds of individual components, each of which was
*  individually copyrighted.  Each Cloudera open source product is a
*  collective work under U.S. Copyright Law. Your license to use the
*  collective work is as provided in your written agreement with
*  Cloudera.  Used apart from the collective work, this file is
*  licensed for your use pursuant to the open source license
*  identified above.
*
*  This code is provided to you pursuant a written agreement with
*  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
*  this code. If you do not have a written agreement with Cloudera nor
*  with an authorized and properly licensed third party, you do not
*  have any rights to access nor to use this code.
*
*  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
*  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
*  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
*  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
*  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
*  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
*  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
*  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
*  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
*  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
*  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
*  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
*  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
*  DATA.
*
* #  Author(s): Paul de Fusco
*************************************************************************** */

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.storage.StorageLevel

object ParquetJoinApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("S3 Parquet Left Join App - One Step At a Time")
      .config("spark.sql.shuffle.partitions", "1000")
      .config("spark.sql.files.maxPartitionBytes", "134217728") // 128MB
      .config("spark.sql.parquet.mergeSchema", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Step 1: Load the base DataFrame
    var result = loadParquet(spark, "s3a://pauldefusco-buk-eb9a0129/data/400Mrows_2kcols_10kparts_dataset_1")
      .persist(StorageLevel.DISK_ONLY)

    // Load and join each dataset one by one
    result = performLeftJoinAndCleanup(spark, result, "s3a://pauldefusco-buk-eb9a0129/data/400Mrows_2kcols_10kparts_dataset_2")
    result = performLeftJoinAndCleanup(spark, result, "s3a://pauldefusco-buk-eb9a0129/data/400Mrows_2kcols_10kparts_dataset_4")
    result = performLeftJoinAndCleanup(spark, result, "s3a://pauldefusco-buk-eb9a0129/data/400Mrows_2kcols_10kparts_dataset_5")
    result = performLeftJoinAndCleanup(spark, result, "s3a://pauldefusco-buk-eb9a0129/data/400Mrows_2kcols_10kparts_dataset_6")

    // Final output
    result.write
      .mode("overwrite")
      .parquet("s3a://pauldefusco-buk-eb9a0129/data/join_poc_output")

    result.unpersist()
    spark.stop()
  }

  def loadParquet(spark: SparkSession, path: String): DataFrame = {
    spark.read.parquet(path)
  }

  def performLeftJoinAndCleanup(spark: SparkSession, baseDF: DataFrame, joinPath: String): DataFrame = {
    val joinDF = loadParquet(spark, joinPath)
    val joined = baseDF.join(joinDF, Seq("unique_id"), "left")
      .persist(StorageLevel.DISK_ONLY)

    // Cleanup
    baseDF.unpersist()
    joinDF.unpersist()

    joined
  }
}
