package spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @auther create by jhy
 * @date 2018/5/2 2:01
 */
public class _04_LoadAndSave_java {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Thread.currentThread().getClass().getName()).setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlc = new SQLContext(jsc);

        Dataset<Row> json = sqlc.read().json("data/students.json");
        json.show();

        Dataset<Row> json1 = sqlc.read().format("json").load("data/students.json");
        json1.show();

//        json.write().mode("append").json("");
//        json.write().save("");

//        Dataset<Row> parquet = sqlc.read().parquet("data/parquet/test.parquet");
//        parquet.show();

//        Dataset<Row> loadJson = sqlc.read().load("data/parquet/test.parquet");
//        loadJson.show();

    }
}
