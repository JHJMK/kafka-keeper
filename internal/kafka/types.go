package kafka

import (
	"encoding/hex"
	"fmt"
	"time"
	"unicode/utf8"
)

type Header struct {
	Key   string
	Value []byte
}

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []Header
}

func (m Message) KeyText() string {
	if m.Key == nil {
		return "(nil)"
	}
	return DisplayBytes(m.Key)
}

func (m Message) ValueText() string {
	if m.Value == nil {
		return "(nil)"
	}
	return DisplayBytes(m.Value)
}

func DisplayBytes(value []byte) string {
	if utf8.Valid(value) {
		return string(value)
	}
	return "0x" + hex.EncodeToString(value)
}

type BrokerInfo struct {
	ID         int32
	Host       string
	Port       int32
	Controller bool
}

type Overview struct {
	ClusterID  string
	Brokers    []BrokerInfo
	TopicCount int
	GroupCount int
	TotalLag   int64
}

type TopicSummary struct {
	Name           string
	PartitionCount int
	Internal       bool
}

type PartitionInfo struct {
	ID          int32
	Leader      int32
	Replicas    []int32
	StartOffset int64
	EndOffset   int64
}

type TopicConfig struct {
	Name      string
	Value     string
	Sensitive bool
	Source    string
}

type TopicDetail struct {
	Summary     TopicSummary
	Partitions  []PartitionInfo
	Configs     []TopicConfig
	ConfigError string
	Groups      []string
}

type GroupSummary struct {
	GroupID     string
	State       string
	MemberCount int
	Coordinator string
	TotalLag    int64
}

type Assignment struct {
	Topic     string
	Partition int32
}

type GroupMemberInfo struct {
	MemberID    string
	ClientID    string
	Host        string
	Assignments []Assignment
}

type GroupOffset struct {
	Topic     string
	Partition int32
	Current   int64
	End       int64
	Lag       int64
}

type GroupDetail struct {
	Summary      GroupSummary
	ProtocolType string
	Protocol     string
	Topics       []string
	Members      []GroupMemberInfo
	Offsets      []GroupOffset
}

type BrowseMode string

const (
	BrowseByOffset BrowseMode = "offset"
	BrowseByTime   BrowseMode = "time"
	BrowseByRange  BrowseMode = "range"
)

type BrowseRequest struct {
	Topic         string
	Mode          BrowseMode
	Partition     int32
	Offset        int64
	Start         time.Time
	End           time.Time
	Limit         int
	AllPartitions bool
}

type SearchRequest struct {
	Topic       string
	Partition   int32 // -1 searches every partition; a non-negative value targets one partition.
	Start       time.Time
	End         time.Time
	OffsetStart *int64
	OffsetEnd   *int64
	Key         string
	KeyContains bool
	HeaderKey   string
	HeaderValue string
	MaxScan     int
	MaxResults  int
}

type SearchResult struct {
	Messages  []Message
	Scanned   int
	Truncated bool
}

type ProduceRequest struct {
	Topic         string
	Partition     int32
	AutoPartition bool
	Key           []byte
	Value         []byte
	Headers       []Header
	Compression   string
	Timestamp     time.Time
}

type ProduceResult struct {
	Topic     string
	Partition int32
	Offset    int64
	Elapsed   time.Duration
}

type ConsumerOptions struct {
	GroupID    string
	Topic      string
	FromStart  bool
	AutoCommit bool
}

type ConsumerRecord struct {
	Message
}

type ConsumerEvent struct {
	Record *ConsumerRecord
	Err    error
}

func (m Message) String() string {
	return fmt.Sprintf("%s/%d@%d", m.Topic, m.Partition, m.Offset)
}
