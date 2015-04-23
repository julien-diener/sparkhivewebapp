A very simple spring web app that use apache spark hive

**Note:** Currently, it is only tested/implemented/working on local disc and spark 'local'.

Author [Julien Diener](http://julien.diener.website)

###Getting started

First, the spark assembly jar (provided in the installation spark lib folder) should be added to maven local repo.
See the command in the *Note on spark dependencies* section below.

**package and install**:

`mvn install`

**run using jetty plugin**:

`mvn jetty:run`

Then got to [http://localhost:8080/run](http://localhost:8080/run)

###Note on spark dependencies

**Spark assembly**

The maven spark dependencies are not usable at run time (well I did not manage to). The solution is to use the spark
assembly jar provided by the spark installation, as it is the actual jar dep that is required to run stand alone app.

To do so, there are 2 solutions:

  1. Add the jar to the generated war. However, it is quite a big jar.
  2. Add it to the servlet container

Here I use the second option. I use the maven jetty plugin (see the [pom.xml file](pom.xml)) and added it into
the plugin dependency. It is thus necessary to first add the spark assembly jar in the maven local repo,
using the following command:

    mvn install:install-file \
       -Dfile=$SPARK_HOME/lib/spark-assembly-1.1.1-hadoop2.4.0.jar \
       -DgroupId=org.apache.spark -DartifactId=spark-assembly-jar \
       -Dversion=1.1.1 -Dpackaging=jar

See this [SO question](http://stackoverflow.com/q/28860270/1206998) for more details.

**datanucleus**

To run hive, the datanucleus core (3.3.2), api-jdo (3.2.1) and rdbms (3.2.1) are added as maven dependencies.
The version were chosen to match those included in `$SPARK_HOME/lib`.