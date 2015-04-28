package org.proto.sparkhivewebapp.webapp;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import org.proto.sparkhivewebapp.shared.MyData;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
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

    private final static File coreSiteFile = new File("hive-site.xml");
    private final static String coreSiteContent = ""+
            "<configuration>\n" +
            "    <property>\n" +
            "        <name>fs.default.name</name>\n" +
            "        <value>%s</value>\n" +
            "   <description>base url of hdfs namenode - given at runtime</description>\n" +
            "    </property>\n" +
            "<property>\n" +
            "   <name>hive.metastore.warehouse.dir</name>\n" +
            "   <value>/tmp/hive/warehouse</value>\n" +
            "   <description>location of the warehouse directory</description>\n" +
            " </property>\n" +
            "</configuration>\n";


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
            //makeHiveSiteFile();

            SparkConf conf = new SparkConf().setAppName("Spark hive prototype").setMaster(master);
            conf.setJars(JavaSparkContext.jarOfClass(MyData.class));

            sparkContext = new JavaSparkContext(conf);
        }

        if(sparkHiveContext == null) {
            sparkHiveContext = new JavaHiveContext(sparkContext);
            sparkHiveContext.sqlContext().setConf("fs.default.name", namenode==null ? "file:///" : namenode);

            // print that the required namenode is used by hive
            HiveConf hiveConf = sparkHiveContext.sqlContext().hiveconf();
            System.out.println("spark hive fs.default.name: " + hiveConf.get("fs.default.name", null));
        }
        System.out.println("-------------------------------");
    }

    private static void makeHiveSiteFile() {
        // create a hive-site.xml file with suitable fs.default.name
        // and add it to the classpath for hive to find it
        // todo: replace this method by making the suitable HiveConf instance, and giving it to HiveContext
        try {
            // the value of fs.default.name
            String hiveFsDefaultName;
            if(namenode!=null) hiveFsDefaultName = namenode;
            else               hiveFsDefaultName = "file:///";

            // write the file
            String content = String.format(coreSiteContent, hiveFsDefaultName);
            //if(!coreSiteFile.exists()) coreSiteFile.createNewFile();
            FileWriter fw = new FileWriter(coreSiteFile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(content);
            bw.close();

            // add path containing file to classpath
            Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            method.setAccessible(true);
            File dir = coreSiteFile.getAbsoluteFile().getParentFile();
            URL dirUrl = dir.toURI().toURL();
            method.invoke(ClassLoader.getSystemClassLoader(), dirUrl);

        }catch(IOException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            e.printStackTrace();  // todo: manage error
        }
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
