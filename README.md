GPUdb Spark Connector
======================

The documentation can be found at http://www.gpudb.com/docs/5.2/index.html. The connector specific documentation can be found at:

*   www.gpudb.com/docs/5.2/connectors/spark_guide.html

For changes to the connector API, please refer to CHANGELOG.md.  For changes to GPUdb functions, please refer to CHANGELOG-FUNCTIONS.md.

-----

-----


Spark Developer Manual
======================

The following guide provides step by step instructions to get started integrating *GPUdb* with *Spark*.

This project is aimed to make *GPUdb Spark* accessible, meaning an ``RDD`` or ``DStream`` can be generated from a *GPUdb* table or can be saved to a *GPUdb* table.

Source code for the connector can be found at https://github.com/GPUdb/gpudb-connector-spark


Connector Classes
-----------------

The three connector classes that integrate *GPUdb* with *Spark* are:

``com.gpudb.spark.input``

* ``GPUdbReader`` - Reads data from a table into an ``RDD``
* ``GPUdbReceiver`` - A *Spark* streaming ``Receiver`` that receives data from a *GPUdb* table monitor stream

``com.gpudb.spark.output``

* ``GPUdbWriter`` - Writes data from an ``RDD`` or ``DStream`` into *GPUdb*


-----


Spark Context
-------------

The *GPUdb Spark* connector uses the *Spark* configuration to pass *GPUdb* instance information to the *Spark* workers. Ensure that the following properties are set in the *Spark* configuration (``SparkConf``) of the *Spark* context (``JavaSparkContext``) when using each of the following connector interfaces:

``GPUdbReader``

* ``gpudb.host`` - The hostname or IP address of the *GPUdb* instance
* ``gpudb.port`` - The port number on which the *GPUdb* service is listening
* ``gpudb.threads`` - The number of threads *GPUdb* should use
* ``gpudb.table`` - The name of the *GPUdb* table being accessed

``GPUdbWriter``

* ``gpudb.host`` - The hostname or IP address of the *GPUdb* instance
* ``gpudb.port`` - The port number on which the *GPUdb* service is listening
* ``gpudb.threads`` - The number of threads *GPUdb* should use
* ``gpudb.table`` - The name of the *GPUdb* table being accessed
* ``gpudb.insert.size`` - The number of records to queue before inserting into *GPUdb*


-----


Loading Data from GPUdb into a Spark RDD
----------------------------------------

To read from a *GPUdb* table, create a class that extends ``RecordObject`` and implements ``Serializable``. For example::

      public class PersonRecord extends RecordObject implements Serializable
      {
         @RecordObject.Column(order = 0, properties = { ColumnProperty.DATA })
         public long id;

         @RecordObject.Column(order = 1) 
         public String name;        

         @RecordObject.Column(order = 2) 
         public long birthDate;   

         public PersonRecord(){}
      }


Note: The column order specified in the class must correspond to the table schema.

Next, instantiate a ``GPUdbReader`` for that class and call the ``readTable`` method with an optional filter ``expression``::

      GPUdbReader<PersonRecord> reader = new GPUdbReader<PersonRecord>(sparkConf);
      JavaRDD<PersonRecord> rdd = reader.readTable(PersonRecord.class, "PeopleTable", expression, sparkContext);

The ``expression`` in the ``readTable`` call is equivalent to a SQL ``WHERE`` clause.  For details, read the *Expressions* section of the *Concepts* page.


-----


Saving Data from a Spark RDD to GPUdb
-------------------------------------
Creating a *GPUdb* table::

      GPUdbUtil.createTable(gpudbUrl, tableName, PersonRecord.class);

Writing to a *GPUdb* table::

      final GPUdbWriter<PersonRecord> writer = new GPUdbWriter<PersonRecord>(sparkConf);
      writer.write(rdd);


-----


Receiving Data from GPUdb into a Spark DStream
----------------------------------------------
The following creates a ``DStream`` from any new data inserted into the table ``tableName``::

      GPUdbReceiver receiver = new GPUdbReceiver(gpudbUrl, gpudbStreamUrl, tableName);

      JavaReceiverInputDStream<AvroWrapper> dstream = javaStreamingContext.receiverStream(receiver);

Each record in the ``DStream`` is of type ``AvroWrapper``, which is an *Avro* object along with its schema to decode it.

Note:  At this time, only ``add`` and ``bulkadd`` functions will trigger the *GPUdb* to publish added records to *ZMQ* to be received by the *Spark* streaming interface.  New records can also be added via the *GPUdb* administration page.


-----


Saving Data from a Spark DStream to GPUdb
-----------------------------------------
Creating a *GPUdb* table::

      GPUdbUtil.createTable(gpudbUrl, tableName, PersonRecord.class);

Writing to a *GPUdb* table::

      final GPUdbWriter<PersonRecord> writer = new GPUdbWriter<PersonRecord>(sparkConf);
      writer.write(dstream);


-----


Examples
--------

Examples can be found in the ``com.gpudb.spark`` package:

* ``BatchExample`` - Reading & writing *GPUdb* data via *Spark* using an ``RDD``
* ``StreamExample`` - Reading & writing *GPUdb* data via *Spark* using a ``DStream``


-----


Installation & Configuration
----------------------------

The example code provided in this project assumes launching will be done on a *Spark* server using ``/bin/spark-submit``.  The ``example.sh`` script can run each example with minimal configuration via the ``example.properites`` file.

To install the example, the *Spark* connector RPM needs to be deployed onto the *Spark* driver host.  The RPM generated by this project should be installed, where ``<X.Y.Z>`` is the *GPUdb* version and ``<YYYYMMDDhhmmss>`` is the build date::

        [root@local]# yum -y install gpudb-connector-spark-<X.Y.Z>-<YYYYMMDDhhmmss>.noarch.rpm

Once this RPM is installed, the following files should exist::

        /opt/gpudb/connectors/spark/example.properties
        /opt/gpudb/connectors/spark/example.sh
        /opt/gpudb/connectors/spark/gpudb-spark-5.2.0.jar
        /opt/gpudb/connectors/spark/gpudb-spark-5.2.0-jar-with-dependencies.jar
        /opt/gpudb/connectors/spark/gpudb-spark-5.2.0-node-assembly.jar
        /opt/gpudb/connectors/spark/gpudb-spark-5.2.0-shaded.jar
        /opt/gpudb/connectors/spark/README.md

The ``gpudb.host`` property in ``example.properties`` should be modified to be the name of the *GPUdb* host being accessed.

To run the example, issue this *Unix* command with no parameters to display usage information::

        [gpudb@local]$ ./example.sh
