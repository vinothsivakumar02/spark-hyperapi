package com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkCreateHyperFromCsv2 {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[1]")
                .appName("SparkCreateHyperFromCsv").getOrCreate();

        /*spark.sparkContext().hadoopConfiguration().set("fs.s3n.access.key", "********");


        // spark.sparkContext().hadoopConfiguration().set("fs.s3n.endpoint", "s3.amazonaws.com");



        /*Dataset<Row> ds  = spark.read().format("csv").option("inferSchema", "true")
                .option("header", "true")
                //.option("fs.s3a.multipart.size", "104857600")
                .load("s3n://mybucket-apr29/states-2.csv");*/


        Dataset<Row> ds  = spark.read().format("com.databricks.spark.csv").option("inferSchema", "true")
                .option("header", "true")
                //.option("fs.s3a.multipart.size", "104857600")
                .load("s3n://mybucket-apr29/customers.csv");
        // mybucket-apr29.s3-ap-southeast-2.amazonaws.com/customers.csv
        ds.show();
        ds.printSchema();



    }
}
