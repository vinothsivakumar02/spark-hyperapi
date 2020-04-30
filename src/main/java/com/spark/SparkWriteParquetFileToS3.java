package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkWriteParquetFileToS3 {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[1]")
                .appName("SparkWriteParquetFileToS3").getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
        spark.sparkContext().hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "********");

        spark.sparkContext().hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "********");

        spark.sparkContext().hadoopConfiguration().set("fs.s3.buffer.dir", "/Users/vinothsivakumar");


        String path = "/Users/vinothsivakumar/git-projects/states.parquet";

        Dataset<Row> ds = spark.read().parquet(path);

        ds.write().parquet("s3n://mybucket-apr29/states.parquet");

    }
}
