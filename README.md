Kinetica Spark Connector
========================

The documentation can be found at http://www.kinetica.com/docs/6.0/index.html.
The connector specific documentation can be found at:

*   <http://www.kinetica.com/docs/6.0/connectors/spark_guide.html>

For changes to the connector API, please refer to ``CHANGELOG.md``.  For changes
to *Kinetica* functions, please refer to ``CHANGELOG-FUNCTIONS.md``.

-----

-----


Spark Developer Manual
======================

The following guide provides step by step instructions to get started
integrating *Kinetica* with *Spark*.

This project is aimed to make *Kinetica* accessible via *Spark*, meaning an
``RDD`` or ``DStream`` can be generated from a *Kinetica* table or can be saved
to a *Kinetica* table.

Source code for the connector can be found at:

* <https://github.com/kineticadb/kinetica-connector-spark>


Connector Classes
-----------------

The three connector classes that integrate *Kinetica* with *Spark* are:

``com.gpudb.spark.input``

* ``GPUdbReader`` - Reads data from a table into an ``RDD``
* ``GPUdbReceiver`` - A *Spark* streaming ``Receiver`` that receives data from a
  *Kinetica* table monitor stream

``com.gpudb.spark.output``

* ``GPUdbWriter`` - Writes data from an ``RDD`` or ``DStream`` into *Kinetica*


-----


Spark Context
-------------

The connector uses the *Spark* configuration to pass *Kinetica* instance
information to the *Spark* workers. Ensure that the following properties are set
in the *Spark* configuration (``SparkConf``) of the *Spark* context
(``JavaSparkContext``) when using each of the following connector interfaces:

``GPUdbReader``

* ``gpudb.host`` - The hostname or IP address of the database instance (head node)
* ``gpudb.port`` - The port number on which the database service is listening
* ``gpudb.threads`` - The number of threads to use for data encoding/decoding operations
* ``gpudb.table`` - The name of the database table being accessed
* ``gpudb.read.size`` - The number of records to read at a time from the database

``GPUdbWriter``

* ``gpudb.host`` - The hostname or IP address of the database instance
* ``gpudb.port`` - The port number on which the database service is listening
* ``gpudb.threads`` - The number of threads to use for data encoding/decoding operations
* ``gpudb.table`` - The name of the database table being accessed
* ``gpudb.insert.size`` - The number of records to queue before inserting into the database

Note: each reader & writer is configured for a specific table.  To access a
different table, a new reader/writer instance needs to be created & configured.

-----


Configuring Kinetica within Spark
---------------------------------

To access *Kinetica* from *Spark*, first configure a ``SparkConf`` with the
necessary parameters for connecting to the database and accessing a specific
source table::

      SparkConf sparkConf = new SparkConf()
      // Set standard config parameters
      ...

      sparkConf
         .set(GPUdbReader.PROP_GPUDB_HOST,  host)
         .set(GPUdbReader.PROP_GPUDB_PORT, String.valueOf(port))
         .set(GPUdbReader.PROP_GPUDB_THREADS, String.valueOf(threads))
         .set(GPUdbReader.PROP_GPUDB_READ_SIZE, String.valueOf(readSize))
         .set(GPUdbReader.PROP_GPUDB_TABLE_NAME,  tableName);


-----


Loading Data from Kinetica into a Spark RDD
-------------------------------------------

To read from a *Kinetica* table, first configure a ``SparkConf``, as detailed
above, with the necessary parameters to point the reader at the source table.

Next, instantiate a ``GPUdbReader`` with that configuration and call the
``readTable`` method with an optional filter ``expression``::

      GPUdbReader reader = new GPUdbReader(sparkConf);
      JavaRDD<Map<String,Object>> rdd = reader.readTable(expression, sparkContext);

The ``expression`` in the ``readTable`` call is similar to a SQL ``WHERE``
clause.  For details, read the *Expressions* section of the *Concepts*
documentation page here:

* <http://www.kinetica.com/docs/6.0/concepts/expressions.html>


-----


Saving Data from a Spark RDD to Kinetica
----------------------------------------

To create a target table for writing data, a utility function is provided that
takes the URL of the *Kinetica* database, the collection & name of the table to
create, and the ``Type`` schema to be used for the table's configuration::

      GPUdbUtil.createTable(gpudbUrl, collectionName, tableName, type);

To write to that table, create a ``GPUdbWriter`` with the ``SparkConf``
configured as directed above, and pass an *RDD* to the ``write`` method.  The
``rdd`` object should be of type ``JavaRDD<Map<String,Object>>`` whose maps
represent column/value pairs for each record to insert::

      final GPUdbWriter writer = new GPUdbWriter(sparkConf);
      writer.write(rdd);


-----


Receiving Data from Kinetica into a Spark DStream
-------------------------------------------------

The following creates a ``DStream`` from any new data inserted into the table
``tableName``, reading from the ``gpudbStreamUrl``, which is the same as the
``gpudbUrl`` except for the streaming port, which defaults to ``9002``::

      GPUdbReceiver receiver = new GPUdbReceiver(gpudbUrl, gpudbStreamUrl, tableName);

      JavaReceiverInputDStream<AvroWrapper> dstream = javaStreamingContext.receiverStream(receiver);

Each record in the ``DStream`` is of type ``AvroWrapper``, which is an *Avro*
object along with its schema to decode it.

**Note**:  At this time, only data inserted into a table will trigger the
database to publish added records to *ZMQ* to be received by the *Spark*
streaming interface.  New records can also be added via the *Kinetica*
administration page.  Updates & deletes will not be published.


-----


Saving Data from a Spark DStream to Kinetica
--------------------------------------------

To create a target table for writing data, a utility function is provided that
takes the URL of the *Kinetica* database, the collection & name of the table to
create, and the ``Type`` schema to be used for the table's configuration::

      GPUdbUtil.createTable(gpudbUrl, collectionName, tableName, type);

To write to that table, create a ``GPUdbWriter`` with the ``SparkConf``
configured as directed above, and pass a *DStream* to the ``write`` method.  The
``dstream`` object should be of type ``JavaDStream<Map<String,Object>>`` whose
maps represent column/value pairs for each record to insert::

      final GPUdbWriter writer = new GPUdbWriter(sparkConf);
      writer.write(dstream);


-----


Examples
--------

Examples can be found in the ``com.gpudb.spark`` package:

* ``BatchExample`` - Reading & writing *Kinetica* data via *Spark* using an ``RDD``
* ``StreamExample`` - Reading & writing *Kinetica* data via *Spark* using a ``DStream``


-----


Installation & Configuration
----------------------------

The example code provided in this project assumes launching will be done on a
*Spark* server using ``/bin/spark-submit``; if ``SPARK_HOME`` is set, it will
be prepended to the command path.  The ``example.sh`` script can run each
example with minimal configuration via the ``example.properites`` file.

To install the example, the *Spark* connector RPM needs to be deployed onto the
*Spark* driver host.  The RPM generated by this project should be installed,
where ``<X.Y.Z>`` is the *Kinetica* version and ``<YYYYMMDDhhmmss>`` is the
build date::

   sudo yum -y install kinetica-connector-spark-<X.Y.Z>-<YYYYMMDDhhmmss>.noarch.rpm

Once this RPM is installed, the following files should exist::

   /opt/gpudb/connectors/spark/example.properties
   /opt/gpudb/connectors/spark/example.sh
   /opt/gpudb/connectors/spark/gpudb-spark-6.0.1.jar
   /opt/gpudb/connectors/spark/gpudb-spark-6.0.1-jar-with-dependencies.jar
   /opt/gpudb/connectors/spark/gpudb-spark-6.0.1-node-assembly.jar
   /opt/gpudb/connectors/spark/gpudb-spark-6.0.1-shaded.jar
   /opt/gpudb/connectors/spark/README.md

The ``gpudb.host`` property in ``example.properties`` should be modified to
be the name of the *Kinetica* host being accessed.

To run the example, issue this *Unix* command with no parameters to display
usage information::

   ./example.sh
