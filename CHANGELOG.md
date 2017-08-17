GPUdb Spark Connector Changelog
===============================

Version 6.0.1 - 2017-08-16
--------------------------

- Updated the Spark connector to run in a distributed yarn configuration
- Switched to BulkInserter for adding records
- Used random names in streaming example
- Created longer delay in streaming example before adding records to the source table, to allow the two to better synchronize
- Made SPARK_HOST optional, using YARN if not specified
- Made SPARK_HOME optional
- Suppressed ZMQ-related warnings
- Updated Spark connector version to 6.0.1


Version 6.0.0 - 2017-01-24
--------------------------

-   Releasing version


Version 5.2.0 - 2016-08-22
--------------------------

-   Updated to new GPUdb API v5.2.0
-   Added batched & partitioned reads & writes
-   Various bug fixes
-   Updated Avro from 1.7.7 to 1.8.0; shaded Avro to not conflict with Spark's Avro
-   Changed run script to always use shaded JAR
-   Updated documentation


Version 5.1.0 - 2016-05-11
--------------------------

-   Updated to new GPUdb API v5.1.0
-   Removed dependence on Twitter feed and active Internet connection
-   Collapsed streaming examples into a single, self-contained GPUdb -> Spark -> GPUdb example
-   Added GPUdb query -> RDD back into example code & documentation
-   Restructured codebase to better align with other GPUdb projects
-   Removed unused files, settings, & ZQL subproject
-   Updated documentation


Version 4.2.0 - 2016-05-06
--------------------------

-   Final 4.X.X branch


Version 4.0.0 - 2015-12-11
--------------------------

-   Initial version
