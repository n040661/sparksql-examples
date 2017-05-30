package com.sankar;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;


public class RDDToDF {

	public static void main(String[] args) throws AnalysisException {
		SparkSession spark = SparkSession
			      .builder()
			      .appName("Java Spark SQL basic example")
			      .master("local")
			      .config("spark.some.config.option", "some-value")
			      .getOrCreate();
		spark.read().text("/path/to/spark/README.md");
		
		spark.stop();
		
	}
}
