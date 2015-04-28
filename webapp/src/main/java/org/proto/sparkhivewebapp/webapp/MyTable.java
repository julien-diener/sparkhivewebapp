package org.proto.sparkhivewebapp.webapp;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import org.proto.sparkhivewebapp.shared.MyData;

import java.util.ArrayList;
import java.util.List;


public class MyTable {

    static JavaSparkContext sparkContext;
    static JavaHiveContext  sparkHiveContext;
    static String master;
    static String namenode;
    final String name;

    public MyTable(String name){
        this.name = name;
    }

    public static void initSpark(String newMaster, String newNamenode){
        if(newMaster==null) newMaster = "local";

        if( (master!=null   && !master.equals(newMaster))     || (master==null)
         || (namenode!=null && !namenode.equals(newNamenode)) || (namenode==null && newNamenode!=null)){
            if(sparkContext!=null) sparkContext.stop();
            sparkContext = null;
            sparkHiveContext = null;
        }

        if(sparkContext == null) {
            System.out.println("new sparkContext: master="+newMaster+", namenode="+newNamenode);

            master = newMaster;
            namenode = newNamenode;

            SparkConf conf = new SparkConf().setAppName("Spark hive prototype").setMaster(master);
            conf.setJars(JavaSparkContext.jarOfClass(MyData.class));

            sparkContext = new JavaSparkContext(conf);
        }

        if(sparkHiveContext == null) {
            sparkHiveContext = new JavaHiveContext(sparkContext);
            sparkHiveContext.sqlContext().setConf("fs.default.name", namenode==null ? "file:///" : namenode);

            // print that the required namenode is actually used
            HiveConf hiveConf = sparkHiveContext.sqlContext().hiveconf();
            System.out.println("spark hive fs.default.name: " + hiveConf.get("fs.default.name", null));
        }
        System.out.println("-------------------------------");
    }

    public void generateTableUsingRDD(int n) {
        ArrayList<MyData> data = new ArrayList<>(n);
        for(int i=0; i<n; i++)
            data.add(new MyData("data_"+i, i));
        JavaRDD<MyData> myRdd = sparkContext.parallelize(data);

        JavaSchemaRDD schemaRDD = sparkHiveContext.applySchema(myRdd, MyData.class);
        schemaRDD.registerTempTable("MyTableSchemaRDDTemplate");

        sparkHiveContext.sql("DROP TABLE "+name);
        sparkHiveContext.sql("CREATE TABLE "+name+" AS SELECT * FROM MyTableSchemaRDDTemplate");
    }

    public List<MyData> request(){
        JavaSchemaRDD myDataRdd = sparkHiveContext.sql("SELECT * FROM "+name);
        return myDataRdd.map(MyData.parseRow).collect();
    }
}
