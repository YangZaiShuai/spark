package spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**Parquet 面向分析业务的列式存储格式
 * 1.跳过不符合条件的数据,只读取需要的数据,降低IO
 * 2.压缩编码 降低磁盘存储空间 一列数据类型是一致的
 * 3.制度去需要的列,支持向量运算,更好地扫描性能
 * @auther create by jhy
 * @date 2018/5/2 14:16
 */
public class _05_ParquetLoadData_java {
    //用编程方式操作Parquet文件
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Thread.currentThread().getClass().getName()).setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlc = new SQLContext(jsc);

        Dataset<Row> parquet = sqlc.read().parquet("data/users.parquet");
        parquet.registerTempTable("users");

        Dataset<Row> sql = sqlc.sql("select *from users");
        sql.show();

        sql.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row v1) throws Exception {
                return "Name"+v1.getString(0);
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }
}
