package spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.List;

/**编程方式指定元数据,RDD转换为DataFrame
 * @auther create by jhy
 * @date 2018/5/2 1:20
 */
public class _03_RDD2DataFrameProgrammatically_java {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName(Thread.currentThread().getClass().getName()).setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlc = new SQLContext(jsc);

        //创建普通的RDD
        JavaRDD<String> lines = jsc.textFile("data/students.txt");

        //第一步,将rdd转换为Row类型
        JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] splited = line.split(",");

                return RowFactory.create(Integer.parseInt(splited[0]), splited[1], Integer.parseInt(splited[2]));
            }
        });

        //动态构造元数据
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(fields);

        // 第三步，使用动态构造的元数据，将RDD转换为DataFrame
        Dataset<Row> studentDF = sqlc.createDataFrame(studentRDD, structType);


        //注册表
        studentDF.registerTempTable("tableB");

        Dataset<Row> teenegerDF = sqlc.sql("select * from tableB");
        teenegerDF.show();


    }
}
