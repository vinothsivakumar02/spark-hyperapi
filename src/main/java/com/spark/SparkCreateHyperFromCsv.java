package com.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.List;

public class SparkCreateHyperFromCsv {


    static class StatesMapper implements MapFunction<Row, StatesVO> {
        private static final long serialVersionUID = -8940709795225426457L;

        @Override
        public StatesVO call(Row row) throws Exception {
            StatesVO vo = new StatesVO();
            vo.setPopulation(row.getString(0));
            vo.setState(row.getString(1));
            return vo;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[1]")
                .appName("SparkCreateHyperFromCsv").getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
        spark.sparkContext().hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "******");

        spark.sparkContext().hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "*******");


        Dataset<Row> ds = spark.read().format("com.databricks.spark.csv").option("inferSchema", "true")
                .option("header", "true")
                //.option("fs.s3a.multipart.size", "104857600")
                .load("s3n://mybucket-apr29/states.csv");
        Dataset<StatesVO> statesVODataset = ds.map(new StatesMapper(), Encoders.bean(StatesVO.class));

        List<StatesVO> voDataset = statesVODataset.collectAsList();
        //InsertDataIntoSingleTable.insertIntoHyper(voDataset);

        statesVODataset.printSchema();


    }
}
