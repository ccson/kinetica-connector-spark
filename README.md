Kinetica Spark Connector
========================

The documentation can be found at http://www.kinetica.com/docs/5.2/index.html. The connector specific documentation can be found at:

*   <http://www.kinetica.com/docs/5.2/connectors/spark_guide.html>

For changes to the connector API, please refer to CHANGELOG.md.  For changes to Kinetica functions, please refer to CHANGELOG-FUNCTIONS.md.

-----

-----


Spark Developer Manual
======================

The following guide provides step by step instructions to get started integrating *Kinetica* with *Spark*.

This project is aimed to make *Kinetica Spark* accessible, meaning an ``RDD`` or ``DStream`` can be generated from a *Kinetica* table or can be saved to a *Kinetica* table.

Source code for the connector can be found at https://github.com/KineticaDB/kinetica-connector-spark


Connector Classes
-----------------

The three connector classes that integrate *Kinetica* with *Spark* are:

``com.gpudb.spark.input``

* ``GPUdbReader`` - Reads data from a table into an ``RDD``
* ``GPUdbReceiver`` - A *Spark* streaming ``Receiver`` that receives data from a *Kinetica* table monitor stream

``com.gpudb.spark.output``

* ``GPUdbWriter`` - Writes data from an ``RDD`` or ``DStream`` into *Kinetica*


-----


Spark Context
-------------

The *Kinetica Spark* connector uses the *Spark* configuration to pass *Kinetica* instance information to the *Spark* workers. Ensure that the following properties are set in the *Spark* configuration (``SparkConf``) of the *Spark* context (``JavaSparkContext``) when using each of the following connector interfaces:

``GPUdbReader``

* ``gpudb.host`` - The hostname or IP address of the *Kinetica* instance
* ``gpudb.port`` - The port number on which the *Kinetica* service is listening
* ``gpudb.threads`` - The number of threads *Kinetica* should use
* ``gpudb.table`` - The name of the *Kinetica* table being accessed
* ``gpudb.read.size`` - The number of records to read at a time from *Kinetica*

``GPUdbWriter``

* ``gpudb.host`` - The hostname or IP address of the *Kinetica* instance
* ``gpudb.port`` - The port number on which the *Kinetica* service is listening
* ``gpudb.threads`` - The number of threads *Kinetica* should use
* ``gpudb.table`` - The name of the *Kinetica* table being accessed
* ``gpudb.insert.size`` - The number of records to queue before inserting into *Kinetica*


-----


Loading Data from Kinetica into a Spark RDD
-------------------------------------------

To read from a *Kinetica* table, create a class that extends ``RecordObject`` and implements ``Serializable``. For example::

		public class PersonRecord extends RecordObject implements Serializable
		{
			@RecordObject.Column(order = 0, properties = { ColumnProperty.DATA })
			public long id;

			@RecordObject.Column(order = 1)
			public String name;

			@RecordObject.Column(order = 2, properties = { ColumnProperty.TIMESTAMP })
			public long birthDate;

			public PersonRecord(){}
		}


Note: The column order specified in the class must correspond to the table schema.

Next, instantiate a ``GPUdbReader`` for that class and call the ``readTable`` method with an optional filter ``expression``::

		GPUdbReader<PersonRecord> reader = new GPUdbReader<PersonRecord>(sparkConf);
		JavaRDD<PersonRecord> rdd = reader.readTable(PersonRecord.class, expression, sparkContext);

The ``expression`` in the ``readTable`` call is equivalent to a SQL ``WHERE`` clause.  For details, read the *Expressions* section of the *Concepts* page.


-----


Saving Data from a Spark RDD to Kinetica
----------------------------------------
Creating a *Kinetica* table::

		GPUdbUtil.createTable(gpudbUrl, collectionName, tableName, PersonRecord.class);

Writing to a *Kinetica* table::

		final GPUdbWriter<PersonRecord> writer = new GPUdbWriter<PersonRecord>(sparkConf);
		writer.write(rdd);


-----


Receiving Data from Kinetica into a Spark DStream
-------------------------------------------------
The following creates a ``DStream`` from any new data inserted into the table ``tableName``::

		GPUdbReceiver receiver = new GPUdbReceiver(gpudbUrl, gpudbStreamUrl, tableName);

		JavaReceiverInputDStream<AvroWrapper> dstream = javaStreamingContext.receiverStream(receiver);

Each record in the ``DStream`` is of type ``AvroWrapper``, which is an *Avro* object along with its schema to decode it.

Note:  At this time, only ``add`` and ``bulkadd`` functions will trigger the *Kinetica* to publish added records to *ZMQ* to be received by the *Spark* streaming interface.  New records can also be added via the *Kinetica* administration page.


-----


Saving Data from a Spark DStream to Kinetica
--------------------------------------------
Creating a *Kinetica* table::

		GPUdbUtil.createTable(gpudbUrl, collectionName, tableName, PersonRecord.class);

Writing to a *Kinetica* table::

		final GPUdbWriter<PersonRecord> writer = new GPUdbWriter<PersonRecord>(sparkConf);
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

The example code provided in this project assumes launching will be done on a *Spark* server using ``/bin/spark-submit``.  The ``example.sh`` script can run each example with minimal configuration via the ``example.properites`` file.

To install the example, the *Spark* connector RPM needs to be deployed onto the *Spark* driver host.  The RPM generated by this project should be installed, where ``<X.Y.Z>`` is the *Kinetica* version and ``<YYYYMMDDhhmmss>`` is the build date::

        [root@local]# yum -y install gpudb-connector-spark-<X.Y.Z>-<YYYYMMDDhhmmss>.noarch.rpm

Once this RPM is installed, the following files should exist::

        /opt/gpudb/connectors/spark/example.properties
        /opt/gpudb/connectors/spark/example.sh
        /opt/gpudb/connectors/spark/gpudb-spark-5.2.0.jar
        /opt/gpudb/connectors/spark/gpudb-spark-5.2.0-jar-with-dependencies.jar
        /opt/gpudb/connectors/spark/gpudb-spark-5.2.0-node-assembly.jar
        /opt/gpudb/connectors/spark/gpudb-spark-5.2.0-shaded.jar
        /opt/gpudb/connectors/spark/README.md

The ``gpudb.host`` property in ``example.properties`` should be modified to be the name of the *Kinetica* host being accessed.

To run the example, issue this *Unix* command with no parameters to display usage information::

        [gpudb@local]$ ./example.sh
