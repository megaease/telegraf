# Kafka Topic Consumer Input Plugin

The [Kafka][kafka] topic consumer plugin is an alternative burrow plugin.

It fetches consumer group offset and lag information from the kafka instead of the burrow.

Support the kafka's verson must greater than 0.10.2.

## Configuration

```toml @sample.conf
# Read consumer group offset and lag from Kafka.
[[inputs.kafka_topic_consumer]]
  ## Kafka brokers.
  brokers = ["localhost:9092"]

  ## Topics interested in offset and lag 
  ## (regexp, default is ".*" represents all topics, )
  topics = ".*"

  ## ConsumerGroups interested in offset and lag 
  ## (regexp, default is ".*" represents all consumer groups)
  consumer_groups = ".*"

  ## ExcludeTopics exclude uninterested topics
  ## (regexp, default is "", represents excluding none topics)
  exclude_topics = "__.*"

  ## Optional Client id
  # client_id = "Telegraf"

  ## Set the minimal supported Kafka version.  Setting this enables the use of new
  ## Kafka features and APIs.  Must be 0.10.2.0 or greater.
  ##   ex: version = "1.1.0"
  # version = ""

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## SASL authentication credentials.  These settings should typically be used
  ## with TLS encryption enabled
  # sasl_username = "kafka"
  # sasl_password = "secret"

  ## Optional SASL:
  ## one of: OAUTHBEARER, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI
  ## (defaults to PLAIN)
  # sasl_mechanism = ""

  ## used if sasl_mechanism is GSSAPI (experimental)
  # sasl_gssapi_service_name = ""
  # ## One of: KRB5_USER_AUTH and KRB5_KEYTAB_AUTH
  # sasl_gssapi_auth_type = "KRB5_USER_AUTH"
  # sasl_gssapi_kerberos_config_path = "/"
  # sasl_gssapi_realm = "realm"
  # sasl_gssapi_key_tab_path = ""
  # sasl_gssapi_disable_pafxfast = false

  ## used if sasl_mechanism is OAUTHBEARER (experimental)
  # sasl_access_token = ""

  ## SASL protocol version.  When connecting to Azure EventHub set to 0.
  # sasl_version = 1

  # Disable Kafka metadata full fetch
  # metadata_full = false
```
## Metrics

### Fields

* `kafka_group_topic` (one event per each consumer group)
  * partition_count (int, `number of partitions`)
  * offset_sum (int64, `total offset of all partitions`)
  * current_offset_sum (int64  `total offset of consumers to consume message`)
  * total_lag (int64, `total lag`)
  * lag (int64, `always equal to total_lag || 0`)
  * timestamp (int64)

* `kafka_topic_partition` (one event per each topic partition)
  * lag (int64, `current_lag || 0`)
  * offset (int64)
  * timestamp (int64)

* `kafka_topic_offset` (one event per topic offset)
  * offset (int64)
  * timestamp (int64)

### Tags

* `kafka_group_topic`
  * group (string)
  * topic (string)

* `kafka_topic_partition`
  * group (string)
  * topic (string)
  * partition (int)

* `kafka_topic_offset`
  * topic (string)
  * partition (int)

### Example

#### kafka_group_topic

```json
{
  "fields":{
    "lag":1,
    "offset_sum":388,
    "partition_count":3,
    "timestamp":1669255740005,
    "total_lag":1
  },
  "name":"kafka_group_topic",
  "tags":{
    "category":"infrastructure",
    "cluster_name":"kafka_3e189edb8ad149a4",
    "deploy_mode":"host",
    "group":"things-data-listener",
    "host":"zhaokun-tm1801",
    "host_ipv4":"172.16.0.28",
    "host_name":"VM-0-28-ubuntu",
    "index":"kafkaserver",
    "instance":"kafka_3e189edb8ad149a4",
    "node_name":"node_03a703046f",
    "service":"kafka_3e189edb8ad149a4",
    "system":"megacloud-kafka",
    "tags":"_metrics",
    "topic":"to_cloud",
    "type":"kafkaserver"
  },
  "timestamp":1669255740
}
```

#### kafka_topic_partition

```json
{
  "fields":{
    "lag":1,
    "offset":137,
    "timestamp":1669255840001
  },
  "name":"kafka_topic_partition",
  "tags":{
    "category":"infrastructure",
    "cluster_name":"kafka_3e189edb8ad149a4",
    "deploy_mode":"host",
    "group":"things-data-listener",
    "host":"zhaokun-tm1801",
    "host_ipv4":"172.16.0.28",
    "host_name":"VM-0-28-ubuntu",
    "index":"kafkaserver",
    "instance":"kafka_3e189edb8ad149a4",
    "node_name":"node_03a703046f",
    "partition":"0",
    "service":"kafka_3e189edb8ad149a4",
    "system":"megacloud-kafka",
    "tags":"_metrics",
    "topic":"to_cloud",
    "type":"kafkaserver"
  },
  "timestamp":1669255840
}
```

#### kafka_topic_offset

```json
{
  "fields":{
    "offset":140,
    "timestamp":1669255890001
  },
  "name":"kafka_topic_offset",
  "tags":{
    "category":"infrastructure",
    "cluster_name":"kafka_3e189edb8ad149a4",
    "deploy_mode":"host",
    "host":"zhaokun-tm1801",
    "host_ipv4":"172.16.0.28",
    "host_name":"VM-0-28-ubuntu",
    "index":"kafkaserver",
    "instance":"kafka_3e189edb8ad149a4",
    "node_name":"node_03a703046f",
    "partition":"1",
    "service":"kafka_3e189edb8ad149a4",
    "system":"megacloud-kafka",
    "tags":"_metrics",
    "topic":"to_cloud",
    "type":"kafkaserver"
  },
  "timestamp":1669255890
}

```

[kafka]: https://kafka.apache.org