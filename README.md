A very simple spring web app that use apache spark hive

**Note:** Currently, it is only tested/implemented/working on local disc and spark 'local'.

Author [Julien Diener](http://julien.diener.website)

Note about spark dependencies
-----------------------------

**Spark assembly**

The maven spark dependencies are not usable at run time (well I did not manage to). The solution is to use the spark
assembly jar provided by the spark installation, as it is the actual jar dep that is required to run stand alone app.

To do so, there are 2 solutions:

  1. Add the jar to the generated war. However, it is quite a big jar.
  2. Add it to the servlet container

Here I use the second option. As I use the maven jetty plugin (see the [pom.xml file](pom.xml)) I added it as
a dependency which needs to be added to the maven local repo with:

    mvn install:install-file \
       -Dfile=$SPARK_HOME/lib/spark-assembly-1.1.1-hadoop2.4.0.jar \
       -DgroupId=org.apache.spark -DartifactId=spark-assembly-jar \
       -Dversion=1.1.1 -Dpackaging=jar

See this [SO question](http://stackoverflow.com/q/28860270/1206998) for more details.

**datanucleus**

To run hive, the datanuleus core (3.3.2), api-jdo (3.2.1) and rdbms (3.2.1) are added as maven dependencies.
The version were chosen to match those included in `$SPARK_HOME/lib`.