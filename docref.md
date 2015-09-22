# Configuration Reference


## Cassandra Authentication Parameters
Property Name | Default | Description
--- | --- | ---
spark.cassandra.auth.conf.factory | com.datastax.spark.connector.cql.DefaultAuthConfFactory$ | name of a Scala module or class implementing AuthConfFactory providing custom authentication configuration
spark.cassandra.auth.password | None | password for password authentication
spark.cassandra.auth.username | None | Login name for password authentication

## Cassandra Connection Parameters
Property Name | Default | Description
--- | --- | ---
spark.cassandra.connection.compression |  | Compression to use (LZ4, SNAPPY or NONE)
spark.cassandra.connection.factory | com.datastax.spark.connector.cql.DefaultConnectionFactory$ | Name of a Scala module or class implementing CassandraConnectionFactory providing connections to the Cassandra cluster
spark.cassandra.connection.host | localhost | Contact point to connect to the Cassandra cluster
spark.cassandra.connection.keep_alive_ms | 250 | Period of time to keep unused connections open
spark.cassandra.connection.local_dc | None | The local DC to connect to (other nodes will be ignored)
spark.cassandra.connection.port | 9042 | Cassandra native connection port
spark.cassandra.connection.reconnection_delay_ms.max | 60000 | Maximum period of time to wait before reconnecting to a dead node
spark.cassandra.connection.reconnection_delay_ms.min | 1000 | Minimum period of time to wait before reconnecting to a dead node
spark.cassandra.connection.timeout_ms | 5000 | Maximum period of time to attempt connecting to a node
spark.cassandra.query.retry.count | 10 | Number of times to retry a timed-out query
spark.cassandra.query.retry.delay | ExponentialDelay(4 seconds,1.5) | The delay between subsequent retries (can be constant,  like 1000; linearly increasing, like 1000+100; or exponential, like 1000*2)
spark.cassandra.read.timeout_ms | 120000 |  Maximum period of time to wait for a read to return 

## Cassandra Dataframe Source Paramters
Property Name | Default | Description
--- | --- | ---
spark.cassandra.table.size.in.bytes | None | Used by DataFrames Internally, will be updated in a future release to retreive size from C*. Can be set manually now

## Cassandra SQL Context Options
Property Name | Default | Description
--- | --- | ---
spark.cassandra.sql.cluster | None | Sets the default Cluster to inherit configuration from
spark.cassandra.sql.keyspace | None | Sets the default keyspace

## Cassandra SSL Connection Options
Property Name | Default | Description
--- | --- | ---
spark.cassandra.connection.ssl.enabled | false | Enable secure connection to Cassandra cluster	
spark.cassandra.connection.ssl.enabledAlgorithms | Set(TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA) | Enable secure connection to Cassandra cluster	
spark.cassandra.connection.ssl.protocol | TLS | SSL protocol
spark.cassandra.connection.ssl.trustStore.password | None | Trust store password
spark.cassandra.connection.ssl.trustStore.path | None | Path for the trust store being used
spark.cassandra.connection.ssl.trustStore.type | JKS | Trust store type

## Read Tuning Parameters
Property Name | Default | Description
--- | --- | ---
spark.cassandra.input.consistency.level | LOCAL_ONE | Consistency level to use when reading	
spark.cassandra.input.fetch.size_in_rows | 1000 | Approx amount of data to be fetched into a Spark partition
spark.cassandra.input.metrics | true | Sets whether to record connector specific metrics on write
spark.cassandra.input.split.size_in_mb | 64 | Approx amount of data to be fetched into a Spark partition

## Write Tuning Parameters
Property Name | Default | Description
--- | --- | ---
spark.cassandra.output.batch.grouping.buffer.size | 1000 |  How many batches per single Spark task can be stored in memory before sending to Cassandra
spark.cassandra.output.batch.grouping.key | Partition | Determines how insert statements are grouped into batches. Available values are: * `none`: a batch may contain any statements * `replica_set`: a batch may contain only statements to be written to the same replica set * `partition` (default): a batch may contain only statements for rows sharing the same partition key value
spark.cassandra.output.batch.size.bytes | 1024 | Maximum total size of the batch in bytes. Overridden by null     
spark.cassandra.output.batch.size.rows | None | Number of rows per single batch. The default is 'auto' which means the connector will adjust the number of rows based on the amount of data in each row
spark.cassandra.output.concurrent.writes | 5 | Maximum number of batches executed in parallel by a  single Spark task
spark.cassandra.output.consistency.level | LOCAL_ONE | Consistency level for writing
spark.cassandra.output.metrics | 1000 | Sets whether to record connector specific metrics on write
spark.cassandra.output.throughput_mb_per_sec | 2147483647 | (Floating points allowed) Maximum write throughput allowed  per single core in MB/s limit this on long (+8 hour) runs to 70% of your max throughput  as seen on a smaller job for stability
