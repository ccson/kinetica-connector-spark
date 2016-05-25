GPUdb Spark Connector
======================

The documentation can be found at http://www.gpudb.com/docs/5.1/index.html. The connector specific documentation can be found at:

*   www.gpudb.com/docs/5.1/connectors/spark_guide.html


Spark Developer Manual
======================

The following guide provides step by step instructions to get started integrating *GPUdb* with *Spark*.  Examples can be found in the ``com.gpudb.spark`` package:

* ``BatchExample`` - Reading & writing *GPUdb* data via *Spark* using an ``RDD``
* ``StreamExample`` - Reading & writing *GPUdb* data via *Spark* using a ``DStream``


-----


Installation & Configuration
----------------------------

This project is aimed to make *GPUdb Spark* accessible, meaning an ``RDD`` or ``DStream`` can be generated from a *GPUdb* table or can be saved to a *GPUdb* table.

The example code provided in this project assumes launching will be done on a *Spark* server using ``spark-submit``.  The ``example.sh`` script can run each example with minimal configuration via the ``example.properites`` file.


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

To read from a *GPUdb* table, first you must create a class that extends ``RecordObject`` and implements ``Serializable``. For example::

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


Note: The column order specified in your class must correspond to the table schema.

Next, instantiate a ``GPUdbReader`` for that class and call the ``readTable`` method with an optional filter ``expression``::

		GPUdbReader<PersonRecord> reader = new GPUdbReader<PersonRecord>(sparkConf);
		JavaRDD<PersonRecord> rdd = reader.readTable(PersonRecord.class, "PeopleTable", expression, sparkContext);

The ``expression`` in the ``readTable`` call is equivalent to a SQL ``select...where`` clause.  For details, read the *Expressions* section of the *Concepts* page.


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

		GPUdbUtil.createTable(gpudbUrl, tableName, TwitterRecord.class);

Writing to a *GPUdb* table::

		final GPUdbWriter<TwitterRecord> writer = new GPUdbWriter<TwitterRecord>(sparkConf);
		writer.write(dstream);

