package spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**创建DataFrame  读取json数据
 * @auther create by jhy
 * @date 2018/4/30 23:36
 */
public class _01_DateFrameCreate_java {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Thread.currentThread().getClass().getName()).setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlc = new SQLContext(jsc);

        Dataset<Row> df = sqlc.read().json("data/students.json");
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(df.col("name"),df.col("age").plus(1)).show();
        df.select(df.col("name"),df.col("age").minus(1)).show();
        df.select(df.col("name"),df.col("age").leq(2)).show();
        df.filter(df.col("age").gt(21)).show();
        df.groupBy("age").count().show();
    }

}
