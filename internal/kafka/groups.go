package kafka

import (
	"context"
	"fmt"
	"sort"

	"github.com/jjk/kafka-keeper/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
)

func (s *Service) ListGroups(ctx context.Context, conn model.Connection) ([]GroupSummary, error) {
	admin, err := s.admin(ctx, conn)
	if err != nil {
		return nil, err
	}
	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, safeError("读取 Consumer Group 列表", err)
	}
	if len(listed) == 0 {
		return []GroupSummary{}, nil
	}
	lags, err := admin.Lag(ctx, listed.Groups()...)
	if err != nil {
		return nil, safeError("读取 Consumer Group Lag", err)
	}
	groups := make([]GroupSummary, 0, len(listed))
	for _, group := range listed.Sorted() {
		lag, ok := lags[group.Group]
		if !ok {
			groups = append(groups, GroupSummary{
				GroupID:     group.Group,
				State:       group.State,
				Coordinator: fmt.Sprint(group.Coordinator),
			})
			continue
		}
		groups = append(groups, groupSummaryFromLag(lag))
	}
	return groups, nil
}

func groupSummaryFromLag(lag kadm.DescribedGroupLag) GroupSummary {
	return GroupSummary{
		GroupID:     lag.Group,
		State:       lag.State,
		MemberCount: len(lag.Members),
		Coordinator: fmt.Sprint(lag.Coordinator.NodeID),
		TotalLag:    lag.Lag.Total(),
	}
}

func (s *Service) LoadGroupDetail(ctx context.Context, conn model.Connection, groupID string) (GroupDetail, error) {
	if groupID == "" {
		return GroupDetail{}, fmt.Errorf("Consumer Group 不能为空")
	}
	admin, err := s.admin(ctx, conn)
	if err != nil {
		return GroupDetail{}, err
	}
	lags, err := admin.Lag(ctx, groupID)
	if err != nil {
		return GroupDetail{}, safeError("读取 Consumer Group", err)
	}
	lag, ok := lags[groupID]
	if !ok {
		return GroupDetail{}, fmt.Errorf("Consumer Group %q 不存在", groupID)
	}
	if err := lag.Error(); err != nil {
		return GroupDetail{}, safeError("读取 Consumer Group", err)
	}

	detail := GroupDetail{
		Summary:      groupSummaryFromLag(lag),
		ProtocolType: lag.ProtocolType,
		Protocol:     lag.Protocol,
		Topics:       make([]string, 0),
		Members:      make([]GroupMemberInfo, 0, len(lag.Members)),
		Offsets:      make([]GroupOffset, 0, len(lag.Lag.Sorted())),
	}
	topicSet := make(map[string]struct{})
	for _, member := range lag.Members {
		info := GroupMemberInfo{
			MemberID: member.MemberID,
			ClientID: member.ClientID,
			Host:     member.ClientHost,
		}
		if assignment, ok := member.Assigned.AsConsumer(); ok {
			for _, topic := range assignment.Topics {
				topicSet[topic.Topic] = struct{}{}
				for _, partition := range topic.Partitions {
					info.Assignments = append(info.Assignments, Assignment{
						Topic:     topic.Topic,
						Partition: partition,
					})
				}
			}
		}
		detail.Members = append(detail.Members, info)
	}
	for _, memberLag := range lag.Lag.Sorted() {
		topicSet[memberLag.Topic] = struct{}{}
		detail.Offsets = append(detail.Offsets, GroupOffset{
			Topic:     memberLag.Topic,
			Partition: memberLag.Partition,
			Current:   memberLag.Commit.At,
			End:       memberLag.End.Offset,
			Lag:       memberLag.Lag,
		})
	}
	for topic := range topicSet {
		detail.Topics = append(detail.Topics, topic)
	}
	sort.Strings(detail.Topics)
	return detail, nil
}
