package spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @auther create by jhy
 * @date 2018/4/19 1:37
 */
public class SparksqlDemo {
    public static void main(String[] args) {

        Logger.getRootLogger().setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("sparksql").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlc = new SQLContext(sc);

        Dataset<Row> json = sqlc.read().json("data/json/test.json");

        json.show();
        json.printSchema();
        json.select(json.col("address"),json.col("count").plus(2));
        json.filter(json.col("count").gt(2365)).show();
        json.groupBy("address").count().show();

        sc.stop();
    }
}
