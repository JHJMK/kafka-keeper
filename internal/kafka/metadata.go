package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jjk/kafka-keeper/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func (s *Service) admin(ctx context.Context, conn model.Connection) (*kadm.Client, error) {
	client, err := s.client(ctx, conn)
	if err != nil {
		return nil, err
	}
	return kadm.NewClient(client.raw), nil
}

func (s *Service) ListTopics(ctx context.Context, conn model.Connection) ([]TopicSummary, error) {
	admin, err := s.admin(ctx, conn)
	if err != nil {
		return nil, err
	}
	metadata, err := admin.Metadata(ctx)
	if err != nil {
		return nil, safeError("读取 Topic 列表", err)
	}
	if err := metadata.Topics.Error(); err != nil {
		return nil, safeError("读取 Topic 列表", err)
	}
	topics := make([]TopicSummary, 0, len(metadata.Topics))
	for _, topic := range metadata.Topics.Sorted() {
		topics = append(topics, TopicSummary{
			Name:           topic.Topic,
			PartitionCount: len(topic.Partitions),
			Internal:       topic.IsInternal,
		})
	}
	return topics, nil
}

func (s *Service) LoadOverview(ctx context.Context, conn model.Connection) (Overview, error) {
	admin, err := s.admin(ctx, conn)
	if err != nil {
		return Overview{}, err
	}
	metadata, err := admin.Metadata(ctx)
	if err != nil {
		return Overview{}, safeError("读取集群概览", err)
	}
	if err := metadata.Topics.Error(); err != nil {
		return Overview{}, safeError("读取集群概览", err)
	}
	brokers := make([]BrokerInfo, 0, len(metadata.Brokers))
	for _, broker := range metadata.Brokers {
		brokers = append(brokers, BrokerInfo{
			ID:         broker.NodeID,
			Host:       broker.Host,
			Port:       broker.Port,
			Controller: broker.NodeID == metadata.Controller,
		})
	}
	groups, err := admin.ListGroups(ctx)
	if err != nil {
		return Overview{}, safeError("读取 Consumer Group 数量", err)
	}
	lags, err := admin.Lag(ctx, groups.Groups()...)
	if err != nil {
		return Overview{}, safeError("读取 Consumer Group Lag", err)
	}
	var totalLag int64
	for _, group := range lags {
		totalLag += group.Lag.Total()
	}
	return Overview{
		ClusterID:  metadata.Cluster,
		Brokers:    brokers,
		TopicCount: len(metadata.Topics),
		GroupCount: len(groups),
		TotalLag:   totalLag,
	}, nil
}

func (s *Service) LoadTopicDetail(ctx context.Context, conn model.Connection, topicName string) (TopicDetail, error) {
	if topicName == "" {
		return TopicDetail{}, fmt.Errorf("Topic 不能为空")
	}
	admin, err := s.admin(ctx, conn)
	if err != nil {
		return TopicDetail{}, err
	}
	metadata, err := admin.Metadata(ctx, topicName)
	if err != nil {
		return TopicDetail{}, safeError("读取 Topic", err)
	}
	topic, ok := metadata.Topics[topicName]
	if !ok {
		return TopicDetail{}, fmt.Errorf("Topic %q 不存在", topicName)
	}
	if topic.Err != nil {
		return TopicDetail{}, safeError("读取 Topic", topic.Err)
	}
	start, err := admin.ListStartOffsets(ctx, topicName)
	if err != nil {
		return TopicDetail{}, safeError("读取起始 Offset", err)
	}
	end, err := admin.ListEndOffsets(ctx, topicName)
	if err != nil {
		return TopicDetail{}, safeError("读取结束 Offset", err)
	}
	var topicConfigs []TopicConfig
	var configError string
	resourceConfigs, err := admin.DescribeTopicConfigs(ctx, topicName)
	if err != nil {
		configError = topicConfigError(topicName, err)
	} else if resource, err := resourceConfigs.On(topicName, nil); err != nil {
		configError = topicConfigError(topicName, err)
	} else if resource.Err != nil {
		configError = topicConfigError(topicName, resource.Err)
	} else {
		topicConfigs = make([]TopicConfig, 0, len(resource.Configs))
		for _, config := range resource.Configs {
			topicConfigs = append(topicConfigs, TopicConfig{
				Name:      config.Key,
				Value:     config.MaybeValue(),
				Sensitive: config.Sensitive,
				Source:    fmt.Sprint(config.Source),
			})
		}
	}
	groups, err := associatedGroups(ctx, admin, topicName)
	if err != nil {
		return TopicDetail{}, err
	}

	detail := TopicDetail{
		Summary: TopicSummary{
			Name:           topic.Topic,
			PartitionCount: len(topic.Partitions),
			Internal:       topic.IsInternal,
		},
		Partitions:  make([]PartitionInfo, 0, len(topic.Partitions)),
		Configs:     topicConfigs,
		ConfigError: configError,
		Groups:      groups,
	}
	for _, partition := range topic.Partitions.Sorted() {
		startOffset, _ := start.Lookup(topicName, partition.Partition)
		endOffset, _ := end.Lookup(topicName, partition.Partition)
		detail.Partitions = append(detail.Partitions, PartitionInfo{
			ID:          partition.Partition,
			Leader:      partition.Leader,
			Replicas:    append([]int32(nil), partition.Replicas...),
			StartOffset: startOffset.Offset,
			EndOffset:   endOffset.Offset,
		})
	}
	return detail, nil
}

func topicConfigError(topic string, err error) string {
	if errors.Is(err, kerr.TopicAuthorizationFailed) {
		return fmt.Sprintf(
			`读取 Topic 配置: Topic %q 无权查看配置，账号缺少 DESCRIBE_CONFIGS 权限（TOPIC_AUTHORIZATION_FAILED）`,
			topic,
		)
	}
	return safeError("读取 Topic 配置", err).Error()
}

func associatedGroups(ctx context.Context, admin *kadm.Client, topic string) ([]string, error) {
	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, safeError("读取关联 Consumer Group", err)
	}
	if len(listed) == 0 {
		return []string{}, nil
	}
	described, err := admin.DescribeGroups(ctx, listed.Groups()...)
	if err != nil {
		return nil, safeError("读取关联 Consumer Group", err)
	}
	groups := make([]string, 0)
	for _, group := range described.Sorted() {
		if slices.Contains(group.JoinTopics(), topic) {
			groups = append(groups, group.Group)
		}
	}
	return groups, nil
}
