package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkReadParquestConvertToHyperFileBAckup {

    public static void main(String[] args) {

        String s3Bucket = args[0];
        String outputPath = args[1];
        String accessKey = args[2];
        String secretKey = args[3];


        SparkSession spark = SparkSession.builder()
                .master("local[1]")
                .appName("SparkCreateHyperFromCsv").getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
        spark.sparkContext().hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIASLB7TOELW6PJJPSX");

        spark.sparkContext().hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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");


        Dataset<Row> ds = spark.read().format("com.databricks.spark.csv").option("inferSchema", "true")
                .option("header", "true")
                //.option("fs.s3a.multipart.size", "104857600")
                .load("s3n://mybucket-apr29/states.parquet");

        ds.show();
        ds.printSchema();

        Dataset<StatesVO> statesVODataset = ds.map(new SparkCreateHyperFromCsv.StatesMapper(), Encoders.bean(StatesVO.class));

        List<StatesVO> voDataset = statesVODataset.collectAsList();
        //InsertDataIntoSingleTable.insertIntoHyper(voDataset);

        statesVODataset.printSchema();
    }
}
