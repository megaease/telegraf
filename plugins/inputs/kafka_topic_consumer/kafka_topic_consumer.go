//go:generate ../../../tools/readme_config_includer/generator
package kafka_topic_consumer

import (
	_ "embed"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/common/kafka"
	"github.com/influxdata/telegraf/plugins/inputs"
)

// DO NOT REMOVE THE NEXT TWO LINES! This is required to embed the sampleConfig data.
//go:embed sample.conf
var sampleConfig string

type (
	kafkaTopicConsumer struct {
		Brokers        []string        `toml:"brokers"`
		ConsumerGroups string          `toml:"consumer_groups"`
		Topics         string          `toml:"topics"`
		ExcludeTopics  string          `toml:"exclude_topics"`
		Log            telegraf.Logger `toml:"-"`

		kafka.ReadConfig
		config *sarama.Config `toml:"-"`

		client sarama.Client `toml:"-"`

		topicFilter   *regexp.Regexp `toml:"-"`
		groupFilter   *regexp.Regexp `toml:"-"`
		excludeTopics *regexp.Regexp `toml:"-"`
	}

	groupLag struct {
		groupID   string
		topic     string
		lagSum    int64
		offsetSum int64
		items     []groupLagItem
	}

	groupLagItem struct {
		partition int32
		lag       int64
		offset    int64
	}
)

func init() {
	inputs.Add("kafka_topic_consumer", func() telegraf.Input {
		return &kafkaTopicConsumer{}
	})
}

// Gather takes in an accumulator and adds the metrics that the Input
// gathers. This is called every agent.interval
func (k *kafkaTopicConsumer) Gather(acc telegraf.Accumulator) error {
	var err error
	var wg = sync.WaitGroup{}

	topics, err := k.client.Topics()
	if err != nil {
		k.Log.Warn("fetch topics info error: %s", err)
		return err
	}

	topicPartitionsOffsets := make(map[string]map[int32]int64)
	for _, topic := range topics {
		if topic == "" || !k.topicFilter.MatchString(topic) || (k.excludeTopics != nil && k.excludeTopics.MatchString(topic)) {
			continue
		}
		topicPartitionsOffsets[topic], err = k.fetchTopicPartitionOffset(topic)
		if err != nil {
			k.Log.Warnf("fetch topic: %s, partition infomation error: %s", topic, err)
			continue
		}
	}

	brokers := k.client.Brokers()
	if len(brokers) == 0 {
		k.Log.Warnf("the kafka has't any brokers")
		return nil
	}
	groupLags := make([]groupLag, 0)
	for _, broker := range brokers {
		wg.Add(1)
		go func(broker *sarama.Broker) {
			defer wg.Done()
			if err := broker.Open(k.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
				k.Log.Errorf("cannot connect to broker %d: %v", broker.ID(), err)
				return
			}

			groupLag := k.fetchLagData(broker, topicPartitionsOffsets)
			groupLags = append(groupLags, groupLag...)
		}(broker)
	}
	wg.Wait()

	k.writeToAccumulator(groupLags, topicPartitionsOffsets, acc)
	return nil
}

func (k *kafkaTopicConsumer) fetchTopicPartitionOffset(topic string) (map[int32]int64, error) {
	partitions, err := k.client.Partitions(topic)
	if err != nil {
		return nil, err
	}
	result := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		currentOffset, err := k.client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			k.Log.Warnf("fetch topic: %s, partition: %d newest offset error: %s", topic, partition, err)
			continue
		}
		result[partition] = currentOffset
	}
	return result, nil

}

func (k *kafkaTopicConsumer) fetchLagData(broker *sarama.Broker, topicPatitionsOffsets map[string]map[int32]int64) []groupLag {
	groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
	if err != nil {
		k.Log.Errorf("cannot get consumer group: %v", err)
		return nil
	}
	groupIds := make([]string, 0)
	for groupId := range groups.Groups {
		if k.groupFilter.MatchString(groupId) {
			groupIds = append(groupIds, groupId)
		}
	}

	result := make([]groupLag, 0)
	for _, group := range groupIds {
		offsetFetchResponse := k.getGroupAllTopicOffset(broker, group, topicPatitionsOffsets)
		for topic, partitions := range offsetFetchResponse.Blocks {
			var currentOffsetSum int64
			var lagSum int64
			partitionConsumed := false
			items := make([]groupLagItem, 0)
			for partition, offsetResponseBlock := range partitions {
				err := offsetResponseBlock.Err
				if err != sarama.ErrNoError {
					k.Log.Errorf("partition %d offset of the topic: %s and consumer group: %s error :%v", partition, topic, group, err.Error())
					continue
				}
				currentOffset := offsetResponseBlock.Offset
				currentOffsetSum += currentOffset
				// consume offset

				if offset, ok := topicPatitionsOffsets[topic][partition]; ok {
					var lag int64
					if offsetResponseBlock.Offset == -1 {
						lag = offset
					} else {
						partitionConsumed = true
						lag = offset - offsetResponseBlock.Offset
					}
					lagSum += lag // no matter what partition
					// lag
					items = append(items, groupLagItem{lag: lag, partition: partition, offset: offset})
				} else {
					k.Log.Warnf("no offset of topic %s partition %d, cannot get consumer group lag", topic, partition)
				}
			}
			if !partitionConsumed {
				k.Log.Debugf("the current group: %s hasn't insterest with the topic: %s , skipd", group, topic)
				continue
			}
			// currentOffsetSum, lagSum (all topic)
			result = append(result, groupLag{topic: topic, groupID: group, lagSum: lagSum, offsetSum: currentOffsetSum, items: items})
		}
	}
	return result

}

func (k *kafkaTopicConsumer) getGroupAllTopicOffset(broker *sarama.Broker, group string, topicPatitionsOffsets map[string]map[int32]int64) *sarama.OffsetFetchResponse {
	offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group, Version: 1}
	for topic, partitions := range topicPatitionsOffsets {
		for partition := range partitions {
			offsetFetchRequest.AddPartition(topic, partition)
		}
	}
	// len(group.members), group.groupId
	offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
	if err != nil {
		k.Log.Errorf("Cannot get offset of group %s: %v", group, err)
		return nil
	}
	return offsetFetchResponse
}

func (k *kafkaTopicConsumer) writeToAccumulator(groupLags []groupLag, topicPatitionsOffsets map[string]map[int32]int64, acc telegraf.Accumulator) {
	timestamp := time.Now().UnixMilli()
	for topic, partitions := range topicPatitionsOffsets {
		for partition, offset := range partitions {
			acc.AddFields("kafka_topic_offset",
				map[string]interface{}{
					"offset":    offset,
					"timestamp": timestamp,
				},
				map[string]string{
					"topic":     topic,
					"partition": strconv.Itoa(int(partition)),
				})
		}
	}

	for _, groupLag := range groupLags {
		for _, item := range groupLag.items {
			acc.AddFields("kafka_topic_partition", //
				map[string]interface{}{
					"lag":       item.lag,
					"offset":    item.offset,
					"timestamp": timestamp,
				}, //
				map[string]string{
					"group":     groupLag.groupID,
					"topic":     groupLag.topic,
					"partition": strconv.FormatInt(int64(item.partition), 10),
				}, //
			)
		}

		acc.AddFields("kafka_group_topic",
			map[string]interface{}{
				"total_lag":       groupLag.lagSum,
				"lag":             groupLag.lagSum,
				"offset_sum":      groupLag.offsetSum,
				"timestamp":       timestamp,
				"partition_count": len(groupLag.items),
			},
			map[string]string{
				"group": groupLag.groupID,
				"topic": groupLag.topic,
			})
	}

}

func (k *kafkaTopicConsumer) SampleConfig() string {
	return sampleConfig
}

func (k *kafkaTopicConsumer) Init() error {

	var err error

	cfg := sarama.NewConfig()
	// Kafka version 0.10.2.0 is required for consumer groups.
	cfg.Version = sarama.V0_10_2_0

	if err := k.SetConfig(cfg); err != nil {
		return err
	}

	k.config = cfg
	k.client, err = sarama.NewClient(k.Brokers, k.config)
	if err != nil {
		k.Log.Errorf("create a kafka client for %+v failed: %s", k.Brokers, err)
		return err
	}

	if k.ConsumerGroups == "" {
		k.ConsumerGroups = ".*"
	}

	if k.Topics == "" {
		k.Topics = ".*"
	}

	k.groupFilter = regexp.MustCompile(k.ConsumerGroups)
	k.topicFilter = regexp.MustCompile(k.Topics)

	if k.ExcludeTopics != "" {
		k.excludeTopics = regexp.MustCompile(k.ExcludeTopics)
	}

	k.Log.Debugf("initialize kafka_topic_consumer input plugin succeed")
	return nil
}
