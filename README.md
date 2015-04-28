A very simple spring web app that use apache spark hive

**Note:** Currently, it is only implemented to work on local disc.

Author [Julien Diener](http://julien.diener.website)

###Getting started

First, the spark assembly jar (provided in the installation spark lib folder) should be added to maven local repo.
See the command in the *Note on spark dependencies* section below.

**package and install**:

    mvn install

**run using jetty plugin**:

To run the code on local hard disc (no hdfs), the folder `/user/hive/warehouse` should be created:

    sudo mkdir -P /user/hive/warehouse
    sudo chown $USER /user/hive/warehouse

Then start jetty

    cd webapp
    mvn jetty:run

And go to [http://localhost:8080/gen](http://localhost:8080/gen), it will create a default table `mytable`
(and a folder in `/user/hive/warehouse`).

####Implemented services

This spring package implements 2 web services:

  - **gen** that generate (possibly override) a table and return the created data as json.
    The (optional) parameters are `name` the name of the table (default "mytable"),
    `n` the number of row to add to the table, `namenode` the hdfs namenode to use (default is "file:///")
    and `master` the spark master to use (default "local")

  - **request** that return a tab le content as json. The (optional) parameter is `name` the name of the table
    to request content from. Note that the master is always local (I just did not implement the option) and
    the namenode is the one used to create the requested table.

**Spark master**:
To use a (real) spark cluster, the spark master url should be given (using the `master` parameter) as it is
written in the *spark master web ui page*.


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