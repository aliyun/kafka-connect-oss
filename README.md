# Kafka Connect OSS

Now, you can use this connector as a sink, to upload data from kafka topics to OSS in Json, Avro or Parquet format.

This connector periodically polls data from Kafka and in turn uploads it to OSS. You can configure a partitioner to split data into chunks, and each chunk of data is represented as an OSS object.The key name encodes the topic, the Kafka partition, and the start offset of this data chunk. If no partitioner is specified in the configuration, the default partitioner which preserves Kafka partitioning is used. The size of each data chunk is determined by the number of records written to OSS and by schema compatibility.
## License
It is under Apache License, Version 2.0, meaning you are pretty much free to use it however you want in whatever way.

## Limitations

Kafka 0.11.0.0 or above(0.11.x, 1.x, 2.x) is required. Other versions(0.10.x or below) will not be supported now.

## Install OSS Connector

### Compile and Deployment

Execute commands below:

```bash
git clone https://github.com/aliyun/kafka-connect-oss.git
cd kafka-connect-oss
mvn clean install -DskipTests
```

The generated jar library(kafka-connect-oss-5.2.0.jar) will be in the target directory, copy it to $KAFKA_HOME/libs of each node.

### Setup Hadoop configuration

This connector depends on hadoop oss filesystem interface to upload data to OSS, so we should add _core-site.xml_ configuration file to **$KAFKA_HOME/config** directory:

```bash
[root@master ~]# cd $KAFKA_HOME
[root@master kafka_2.11-0.11.0.0]# ls config
connect-console-sink.properties    connect-file-sink.properties    connect-standalone.properties  log4j.properties     tools-log4j.properties
connect-console-source.properties  connect-file-source.properties  consumer.properties            producer.properties  zookeeper.properties
connect-distributed.properties     connect-log4j.properties        core-site.xml                  server.properties
```

Then, we should add $KAFKA_HOME/config to CLASSPATH by changing **$KAFKA_HOME/bin/kafka-run-class.sh** before we start kafka connector cluster.

Add this command to **$KAFKA_HOME/bin/kafka-run-class.sh**.
```bash
[root@master kafka_2.11-0.11.0.0]# vim $KAFKA_HOME/bin/kafka-run-class.sh
```

```
CLASSPATH=$CLASSPATH:$base_dir/config
```

We can see the diff(bin/kafka-run-class.sh.bak is original file)
```bash
[root@master kafka_2.11-0.11.0.0]# diff -y bin/kafka-run-class.sh bin/kafka-run-class.sh.bak
```

```diff
CLASSPATH=$CLASSPATH:$base_dir/config			               <
# Launch mode							                        # Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then				            if [ "x$DAEMON_MODE" = "xtrue" ]; then
  nohup $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $K	  nohup $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $K
else								                            else
  exec $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KA	  exec $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KA
fi								                                fi

```

You should add configurations below to core-site.xml

configuration | value | comments
------- | ------- | -------
fs.oss.endpoint | e.g. oss-cn-zhangjiakou-internal.aliyuncs.com | endpoint to connect to, [endpoints](https://help.aliyun.com/document_detail/31837.html)
fs.oss.accessKeyId |  | access key id
fs.oss.accessKeySecret |  | access key secret
fs.oss.impl | org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem | hadoop oss filesystem implementation
fs.oss.buffer.dir | /tmp/oss | Comma separated list of directories to buffer OSS data before uploading to Aliyun OSS
fs.oss.connection.secure.enabled | false | Connect to oss over ssl or not, true by default
fs.oss.connection.maximum | 2048 | Number of simultaneous connections to oss

You can find the details [**here**](https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aliyun/src/site/markdown/tools/hadoop-aliyun/index.md)

### Setup Kafka connector
You should setup kafka connector cluster, please refer to official documentation here: [https://docs.confluent.io/current/connect/userguide.html#distributed-worker-configuration](https://docs.confluent.io/current/connect/userguide.html#distributed-worker-configuration).

You should configure `bootstrap.servers` and `group.id` in $KAFKA_HOME/config/connect-distributed.properties of each node.
Each node in kafka connector cluster should have the same `group.id`, and then execute command below in each node:

```bash
cd $KAFKA_HOME && nohup bin/connect-distributed.sh config/connect-distributed.properties &
```

### Other configurations

In order to upload data to OSS, you should add `oss.bucket` configuration when you post connector tasks(see Quick Start below).

In order to use ParquetJsonFormat, you should add `oss.parquet.protobuf.schema.class` configuration when you post connector tasks(see Quick Start below).
`oss.parquet.protobuf.schema.class` is a list, the first value is the kafka topic name, and the second value is the schema class of the kafka topic.

## Documentation

### Formats supported
In order to use these formats, you should configure `value.converter` in $KAFKA_HOME/config/connect-distributed.properties and `format.class` when you post connector tasks to kafka connect cluster.

format| format.class | value.converter
------- | ------- | -------
Byte array|com.aliyun.oss.connect.kafka.format.bytearray.ByteArrayFormat|org.apache.kafka.connect.converters.ByteArrayConverter
Avro|com.aliyun.oss.connect.kafka.format.avro.AvroFormat|io.confluent.connect.avro.AvroConverter
Json|com.aliyun.oss.connect.kafka.format.json.JsonFormat|org.apache.kafka.connect.json.JsonConverter
Avro to Parquet|com.aliyun.oss.connect.kafka.format.parquet.ParquetAvroFormat|io.confluent.connect.avro.AvroConverter
Json to Parquet|com.aliyun.oss.connect.kafka.format.parquet.ParquetJsonFormat|org.apache.kafka.connect.storage.StringConverter

#### Details of ParquetJsonFormat
In order to convert json format to parquet format, we need schema of json input. So you should define your schema in google protocol buffer language.

We have an example in _**src/main/proto/parquet.proto**_

There are two ways to provide schema classes:
- You can simply put your schema proto file under src/main/proto/ and compile with this repo(**Recommended**).
- You can compile proto file by yourself, package it and put the final jar in CLASSPATH($KAFKA_HOME/libs)

### Exactly-once delivery

The OSS connector is able to provide exactly-once semantics to consumers of the objects it exports to OSS, under the condition that the connector is supplied with a deterministic partitioner.

Currently, out of the available partitioners, the default and field partitioners are always deterministic. `TimeBasedPartitioner` can be deterministic with some configurations, discussed below. This implies that, when any of these partitioners is used, splitting of files always happens at the same offsets for a given set of Kafka records. These partitioners take into account `flush.size` and `schema.compatibility` to decide when to roll and save a new file to OSS. The connector always delivers files in OSS that contain the same records, even under the presence of failures. If a connector task fails before an upload completes, the file does not become visible to OSS. If, on the other hand, a failure occurs after the upload has completed but before the corresponding offset is committed to Kafka by the connector, then a re-upload will take place. However, such a re-upload is transparent to the user of the OSS bucket, who at any time will have access to the same records made eventually available by successful uploads to OSS.

To guarantee exactly-once semantics with the `TimeBasedPartitioner`, the connector must be configured to use a deterministic implementation of `TimestampExtractor` and a deterministic rotation strategy. The deterministic timestamp extractors are Kafka records (`timestamp.extractor=Record`) or record fields (`timestamp.extractor=RecordField`). The deterministic rotation strategy configuration is `rotate.interval.ms` (setting `rotate.schedule.interval.ms` is non deterministic and will invalidate exactly-once guarantees).

### Schema Evolution

The OSS connector supports schema evolution and reacts to schema changes of data according to the `schema.compatibility` configuration.

- **NO Compatibility**: Default value. This case will ensure that each file written to OSS has the proper schema. When the connector observes a schema change in data, it commits the current set of files for the affected topic partitions and writes the data with new schema in new files
- **BACKWARD Compatibility**: If a schema is evolved in a backward compatible way, we can always use the latest schema to query all the data uniformly. For example, removing fields is backward compatible change to a schema, since when we encounter records written with the old schema that contain these fields we can just ignore them. Adding a field with a default value is also backward compatible.

    If `BACKWARD` is specified in the `schema.compatibility`, the connector keeps track of the latest schema used in writing data to OSS, and if a data record with a schema version larger than current latest schema arrives, the connector commits the current set of files and writes the data record with new schema to new files. For data records arriving at a later time with schema of an earlier version, the connector projects the data record to the latest schema before writing to the same set of files to OSS.
- **FORWARD Compatibility**:  If a schema is evolved in a forward compatible way, we can always use the oldest schema to query all the data uniformly. Removing a field that had a default value is forward compatible, since the old schema will use the default value when the field is missing.

    If `FORWARD` is specified in the `schema.compatibility`, the connector projects the data to the oldest schema before writing to the same set of files to OSS.
- **Full Compatibility**: Full compatibility means that old data can be read with the new schema and new data can also be read with the old schema.

    If `FULL` is specified in the `schema.compatibility`, the connector performs the same action as `BACKWARD`.

Schema evolution in the OSS connector works in the same way as in the [HDFS connector](https://docs.confluent.io/current/connect/kafka-connect-hdfs/index.html)

### Pluggable Partitioner

The connector comes out of the box with partitioners that support default partitioning based on Kafka partitions, field partitioning, and time-based partitioning in days or hours. You may implement your own partitioners by extending the `Partitioner` class. Additionally, you can customize time based partitioning by extending the `TimeBasedPartitioner` class.

## Quick start(Using ParquetJsonFormat as an example)
First, we should create a topic to test.

```bash
[root@master ~]# cd $KAFKA_HOME
[root@master kafka_2.11-0.11.0.0]# bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic test
```

Assume we have 4 nodes cluster(192.168.0.172, 192.168.0.173, 192.168.0.174, 192.168.0.175), and we post connector task:
```bash
curl -i -X POST \
   -H "Accept:application/json" \
   -H "Content-Type:application/json" \
   -d \ '{
     "name":"oss-sink",
     "config": {
        "name":"oss-sink",
        "topics":"test",
        "oss.parquet.protobuf.schema.class":"test,com.aliyun.oss.connect.kafka.format.parquet.Parquet$Demo",
        "connector.class":"com.aliyun.oss.connect.kafka.OSSSinkConnector",
        "format.class":"com.aliyun.oss.connect.kafka.format.parquet.ParquetJsonFormat",
        "flush.size":"10000",
        "tasks.max":"4",
        "storage.class":"com.aliyun.oss.connect.kafka.storage.OSSStorage",
        "partitioner.class":"io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
        "timestamp.extractor":"Record",
        "oss.bucket":"hadoop-oss-test",
        "partition.duration.ms":"10000",
        "path.format":"YYYY-MM-dd-HH",
        "locale":"US",
        "timezone":"Asia/Shanghai",
        "rotate.interval.ms":"30000"
        }}' \
    'http://slave01:8083/connectors'
```

Test schema class(com.aliyun.oss.connect.kafka.format.parquet.Parquet$Demo) has already been compiled in the jar library(kafka-connect-oss-5.2.0.jar)
```
HTTP/1.1 201 Created
Date: Wed, 12 Jun 2019 16:40:19 GMT
Location: http://slave01:8083/connectors/oss-sink
Content-Type: application/json
Content-Length: 836
Server: Jetty(9.2.15.v20160210)
{
	"name": "oss-sink",
	"config": {
		"name": "oss-sink",
		"topics": "test",
		"oss.parquet.protobuf.schema.class": "test,com.aliyun.oss.connect.kafka.format.parquet.Parquet$Demo",
		"connector.class": "com.aliyun.oss.connect.kafka.OSSSinkConnector",
		"format.class": "com.aliyun.oss.connect.kafka.format.parquet.ParquetJsonFormat",
		"flush.size": "10000",
		"tasks.max": "4",
		"storage.class": "com.aliyun.oss.connect.kafka.storage.OSSStorage",
		"partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
		"timestamp.extractor": "Record",
		"oss.bucket": "hadoop-oss-test",
		"partition.duration.ms": "10000",
		"path.format": "YYYY-MM-dd-HH",
		"locale": "US",
		"timezone": "Asia/Shanghai",
		"rotate.interval.ms": "30000"
	},
	"tasks": [{
		"connector": "oss-sink",
		"task": 0
	}, {
		"connector": "oss-sink",
		"task": 1
	}, {
		"connector": "oss-sink",
		"task": 2
	}, {
		"connector": "oss-sink",
		"task": 3
	}]
}
```

Then, we can check status of this connector task by:
```bash
 curl -i -X GET \
   -H "Accept:application/json" \
   -H "Content-Type:application/json" \
 'http://slave01:8083/connectors/oss-sink/status'
```

```
HTTP/1.1 200 OK
Date: Wed, 12 Jun 2019 16:41:18 GMT
Content-Type: application/json
Content-Length: 334
Server: Jetty(9.2.15.v20160210)
{
	"name": "oss-sink",
	"connector": {
		"state": "RUNNING",
		"worker_id": "192.168.0.174:8083"
	},
	"tasks": [{
		"state": "RUNNING",
		"id": 0,
		"worker_id": "192.168.0.173:8083"
	}, {
		"state": "RUNNING",
		"id": 1,
		"worker_id": "192.168.0.172:8083"
	}, {
		"state": "RUNNING",
		"id": 2,
		"worker_id": "192.168.0.175:8083"
	}, {
		"state": "RUNNING",
		"id": 3,
		"worker_id": "192.168.0.174:8083"
	}]
}
```

Then, we can write json data to kafka topic and see results from logs(You can also check from OSS console):
```
[2019-06-12 16:50:11,806] INFO Start to commit file oss://hadoop-oss-test/topics/test/2019-06-12-16/test+0+0000154000.parquet (com.aliyun.oss.connect.kafka.format.parquet.Parqu
etJsonRecordWriterProvider:131)
[2019-06-12 16:50:11,806] INFO Flushing mem columnStore to file. allocated memory: 1299286 (org.apache.parquet.hadoop.InternalParquetRecordWriter:165)
[2019-06-12 16:50:11,877] INFO File oss://hadoop-oss-test/topics/test/2019-06-12-16/test+0+0000154000.parquet committed (com.aliyun.oss.connect.kafka.format.parquet.ParquetJson
RecordWriterProvider:133)
```