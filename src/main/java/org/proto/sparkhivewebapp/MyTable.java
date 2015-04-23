package org.proto.sparkhivewebapp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

import java.util.ArrayList;
import java.util.List;


public class MyTable {

    static JavaSparkContext sparkContext;
    static JavaHiveContext  sparkHiveContext;
    static String master;
    static String namenode;

    public MyTable(){
        generateTableUsingRDD(10);
    }

    public static void initSpark(String newMaster, String newNamenode){
        if(newMaster==null) newMaster = "local";

        if(sparkContext!=null && master!=null && !master.equals(newMaster)){
            sparkContext.stop();
            sparkContext = null;
            sparkHiveContext = null;
        }
        if(sparkContext == null) {
            master = newMaster;
            SparkConf conf = new SparkConf().setAppName("Spark hive prototype").setMaster(master);
            conf.setJars(JavaSparkContext.jarOfClass(MyTable.class));

            sparkContext = new JavaSparkContext(conf);
        }

        if(sparkHiveContext == null) {
            sparkHiveContext = new JavaHiveContext(sparkContext);
        }

        namenode = newNamenode;
    }

    public void generateTableUsingRDD(int n) {
        ArrayList<MyData> data = new ArrayList<>(n);
        for(int i=0; i<n; i++)
            data.add(new MyData("data_"+i, i));
        JavaRDD<MyData> myRdd = sparkContext.parallelize(data);

        JavaSchemaRDD schemaRDD = sparkHiveContext.applySchema(myRdd, MyData.class);
        schemaRDD.registerTempTable("myTableTemplate");

        //sparkHiveContext.sql("CREATE TABLE IF NOT EXISTS mytable (name String, value INT)");
        //sparkHiveContext.sql("INSERT INTO TABLE mytable SELECT * FROM myTableTemplate");

        sparkHiveContext.sql("DROP TABLE myTable");
        sparkHiveContext.sql("CREATE TABLE myTable AS SELECT * FROM myTableTemplate");
    }

    public List<MyData> request(){
        JavaSchemaRDD myDataRdd = sparkHiveContext.sql("SELECT * FROM myTable");
        return myDataRdd.map(MyData.parseRow).collect();
    }
}
