package app

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jjk/kafka-keeper/internal/kafka"
	"github.com/jjk/kafka-keeper/internal/model"
)

func (a *App) RefreshAll() {
	a.RefreshOverview()
	a.RefreshTopics()
	a.RefreshGroups()
}

func (a *App) refreshPage(page Page) {
	switch page {
	case PageOverview:
		a.RefreshOverview()
	case PageTopics:
		a.RefreshTopics()
		if a.Snapshot().SelectedTopic != "" {
			a.RefreshTopicDetail()
		}
	case PageGroups:
		a.RefreshGroups()
		if a.Snapshot().SelectedGroup != "" {
			a.RefreshGroupDetail()
		}
	}
}

func (a *App) StartAutoRefresh(seconds string) {
	state := a.Snapshot()
	if state.ActiveConnectionID == "" {
		a.setError("请先选择 Kafka 连接")
		return
	}
	if state.Page == PageSend {
		a.setError("发送消息页面不支持自动刷新")
		return
	}
	interval, err := parseAutoRefreshInterval(seconds)
	if err != nil {
		a.setError(err.Error())
		return
	}

	a.stopAutoRefresh()
	ctx, cancel := context.WithCancel(context.Background())
	a.requestMu.Lock()
	if a.closed {
		a.requestMu.Unlock()
		cancel()
		return
	}
	a.autoRefreshCancel = cancel
	a.requestMu.Unlock()
	page := state.Page
	a.update(func(state *State) {
		state.RefreshSeconds = strings.TrimSpace(seconds)
		state.AutoRefreshEnabled = true
		state.AutoRefreshPage = page
		state.Error = ""
	})
	go runAutoRefresh(ctx, interval, func() {
		current := a.Snapshot()
		if current.AutoRefreshEnabled && current.AutoRefreshPage == page && current.Page == page {
			a.refreshPage(page)
		}
	})
}

func (a *App) StopAutoRefresh() {
	a.stopAutoRefresh()
}

func (a *App) stopAutoRefresh() {
	a.requestMu.Lock()
	if a.autoRefreshCancel != nil {
		a.autoRefreshCancel()
		a.autoRefreshCancel = nil
	}
	a.requestMu.Unlock()
	a.update(func(state *State) {
		state.AutoRefreshEnabled = false
		state.AutoRefreshPage = ""
	})
}

func parseAutoRefreshInterval(raw string) (time.Duration, error) {
	seconds, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || seconds <= 0 || seconds > int64(^uint64(0)>>1)/int64(time.Second) {
		return 0, fmt.Errorf("自动刷新间隔必须是正整数秒")
	}
	return time.Duration(seconds) * time.Second, nil
}

func runAutoRefresh(ctx context.Context, interval time.Duration, fn func()) {
	if interval <= 0 {
		return
	}
	fn()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}

func autoRefreshButtonLabel(state State) string {
	if state.AutoRefreshEnabled {
		return "停止自动刷新"
	}
	return "自动刷新"
}

func (a *App) beginRequest(key string) context.Context {
	a.requestMu.Lock()
	if a.closed {
		a.requestMu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	if cancel := a.requestCancels[key]; cancel != nil {
		cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.requestCancels[key] = cancel
	a.requestMu.Unlock()
	return ctx
}

func (a *App) cancelRequest(key string) {
	a.requestMu.Lock()
	if cancel := a.requestCancels[key]; cancel != nil {
		cancel()
		delete(a.requestCancels, key)
	}
	a.requestMu.Unlock()
}

func (a *App) currentConnection() (model.Connection, bool) {
	return a.ActiveConnection()
}

func (a *App) RefreshOverview() {
	connection, ok := a.currentConnection()
	if !ok || a.kafka == nil {
		return
	}
	ctx := a.beginRequest("overview")
	a.update(func(state *State) {
		state.OverviewLoading = true
		state.OverviewError = ""
	})
	go func() {
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		overview, err := a.kafka.LoadOverview(requestCtx, connection)
		a.update(func(state *State) {
			if state.ActiveConnectionID != connection.ID {
				return
			}
			state.OverviewLoading = false
			if err != nil {
				state.OverviewError = err.Error()
				return
			}
			state.Overview = overview
			state.OverviewReady = true
			state.StatusText = "已连接 " + connection.Name
		})
	}()
}

func (a *App) RefreshTopics() {
	connection, ok := a.currentConnection()
	if !ok || a.kafka == nil {
		return
	}
	ctx := a.beginRequest("topics")
	a.update(func(state *State) {
		state.TopicsLoading = true
		state.TopicsError = ""
	})
	go func() {
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		topics, err := a.kafka.ListTopics(requestCtx, connection)
		a.update(func(state *State) {
			if state.ActiveConnectionID != connection.ID {
				return
			}
			state.TopicsLoading = false
			if err != nil {
				state.TopicsError = err.Error()
				return
			}
			state.Topics = topics
			state.Produce.Topic = keepKnownValue(state.Produce.Topic, topicNames(topics))
		})
	}()
}

func (a *App) RefreshGroups() {
	connection, ok := a.currentConnection()
	if !ok || a.kafka == nil {
		return
	}
	ctx := a.beginRequest("groups")
	a.update(func(state *State) {
		state.GroupsLoading = true
		state.GroupsError = ""
	})
	go func() {
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		groups, err := a.kafka.ListGroups(requestCtx, connection)
		a.update(func(state *State) {
			if state.ActiveConnectionID != connection.ID {
				return
			}
			state.GroupsLoading = false
			if err != nil {
				state.GroupsError = err.Error()
				return
			}
			state.Groups = groups
		})
	}()
}

func (a *App) SelectTopic(topic string) {
	a.update(func(state *State) {
		state.SelectedTopic = topic
		state.TopicDetail = kafka.TopicDetail{}
		state.TopicDetailError = ""
		state.BrowseResults = nil
		state.BrowseError = ""
		state.BrowseLoaded = false
		state.SearchResults = nil
		state.SearchError = ""
		state.SearchLoaded = false
		state.SelectedMessage = 0
		state.Browse.Partition = "0"
		state.Browse.AllPartitions = true
		state.Consumer.Topic = topic
		state.Produce.Topic = topic
	})
	a.RefreshTopicDetail()
}

func (a *App) RefreshTopicDetail() {
	connection, ok := a.currentConnection()
	topic := a.Snapshot().SelectedTopic
	if !ok || a.kafka == nil || topic == "" {
		return
	}
	ctx := a.beginRequest("topic")
	a.update(func(state *State) {
		state.TopicDetailLoading = true
		state.TopicDetailError = ""
	})
	go func() {
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		detail, err := a.kafka.LoadTopicDetail(requestCtx, connection, topic)
		a.update(func(state *State) {
			if state.ActiveConnectionID != connection.ID || state.SelectedTopic != topic {
				return
			}
			state.TopicDetailLoading = false
			if err != nil {
				state.TopicDetailError = err.Error()
				return
			}
			state.TopicDetail = detail
		})
	}()
}

func (a *App) BrowseMessages() {
	connection, ok := a.currentConnection()
	if !ok || a.kafka == nil {
		return
	}
	state := a.Snapshot()
	if state.SelectedTopic == "" {
		a.setError("请先选择 Topic")
		return
	}
	request := kafka.BrowseRequest{
		Topic:         state.SelectedTopic,
		AllPartitions: state.Browse.AllPartitions,
	}
	switch state.Browse.Mode {
	case "time":
		request.Mode = kafka.BrowseByTime
	case "range":
		request.Mode = kafka.BrowseByRange
	default:
		request.Mode = kafka.BrowseByOffset
	}
	if request.AllPartitions {
		request.Partition = -1
	} else {
		partition, err := parseNonNegativeInt32(state.Browse.Partition, "Partition")
		if err != nil {
			a.setError(err.Error())
			return
		}
		request.Partition = partition
	}
	if request.Mode == kafka.BrowseByOffset {
		offset, err := parseNonNegativeInt64(state.Browse.Offset, "Offset")
		if err != nil {
			a.setError(err.Error())
			return
		}
		request.Offset = offset
	} else {
		if state.Browse.Start.IsZero() {
			a.setError("开始时间不能为空")
			return
		}
		request.Start = state.Browse.Start
		if request.Mode == kafka.BrowseByRange {
			if state.Browse.End.IsZero() {
				a.setError("结束时间不能为空")
				return
			}
			request.End = state.Browse.End
		}
	}
	limit, err := parsePositiveInt(state.Browse.Limit, "条数", 500)
	if err != nil {
		a.setError(err.Error())
		return
	}
	request.Limit = limit

	ctx := a.beginRequest("browse")
	a.update(func(state *State) {
		state.BrowseLoading = true
		state.BrowseError = ""
		state.Error = ""
	})
	go func() {
		requestCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		messages, err := a.kafka.BrowseMessages(requestCtx, connection, request)
		a.update(func(state *State) {
			if state.ActiveConnectionID != connection.ID || state.SelectedTopic != request.Topic {
				return
			}
			state.BrowseLoading = false
			state.BrowseLoaded = true
			if err != nil {
				state.BrowseError = err.Error()
				return
			}
			state.BrowseResults = messages
			state.SelectedMessage = 0
		})
	}()
}

func (a *App) SearchMessages() {
	connection, ok := a.currentConnection()
	if !ok || a.kafka == nil {
		return
	}
	state := a.Snapshot()
	if state.SelectedTopic == "" {
		a.setError("请先选择 Topic")
		return
	}
	partition := int32(-1)
	var err error
	if strings.TrimSpace(state.Search.Partition) != "" {
		partition, err = parsePartition(state.Search.Partition)
		if err != nil {
			a.setError(err.Error())
			return
		}
	}
	request := kafka.SearchRequest{
		Topic:       state.SelectedTopic,
		Partition:   partition,
		Key:         state.Search.Key,
		KeyContains: state.Search.KeyContains,
		HeaderKey:   state.Search.HeaderKey,
		HeaderValue: state.Search.HeaderValue,
	}
	if state.Search.StartEnabled {
		if state.Search.Start.IsZero() {
			a.setError("开始时间不能为空")
			return
		}
		request.Start = state.Search.Start
	}
	if state.Search.EndEnabled {
		if state.Search.End.IsZero() {
			a.setError("结束时间不能为空")
			return
		}
		request.End = state.Search.End
	}
	if state.Search.OffsetStart != "" {
		value, err := parseNonNegativeInt64(state.Search.OffsetStart, "起始 Offset")
		if err != nil {
			a.setError(err.Error())
			return
		}
		request.OffsetStart = &value
	}
	if state.Search.OffsetEnd != "" {
		value, err := parseNonNegativeInt64(state.Search.OffsetEnd, "结束 Offset")
		if err != nil {
			a.setError(err.Error())
			return
		}
		request.OffsetEnd = &value
	}
	request.MaxScan, err = parsePositiveInt(state.Search.MaxScan, "最大扫描数", 100000)
	if err != nil {
		a.setError(err.Error())
		return
	}
	request.MaxResults, err = parsePositiveInt(state.Search.MaxResults, "最大结果数", 500)
	if err != nil {
		a.setError(err.Error())
		return
	}

	ctx := a.beginRequest("search")
	a.update(func(state *State) {
		state.SearchLoading = true
		state.SearchError = ""
		state.Error = ""
	})
	go func() {
		requestCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		result, err := a.kafka.SearchMessages(requestCtx, connection, request)
		a.update(func(state *State) {
			if state.ActiveConnectionID != connection.ID || state.SelectedTopic != request.Topic {
				return
			}
			state.SearchLoading = false
			state.SearchLoaded = true
			if err != nil {
				state.SearchError = err.Error()
				return
			}
			state.SearchResults = result.Messages
			state.SearchScanned = result.Scanned
			state.SearchTruncated = result.Truncated
			state.SelectedMessage = 0
		})
	}()
}

func (a *App) SelectGroup(groupID string) {
	a.update(func(state *State) {
		state.SelectedGroup = groupID
		state.GroupDetail = kafka.GroupDetail{}
		state.GroupDetailError = ""
		state.Consumer.GroupID = groupID
	})
	a.RefreshGroupDetail()
}

func (a *App) RefreshGroupDetail() {
	connection, ok := a.currentConnection()
	groupID := a.Snapshot().SelectedGroup
	if !ok || a.kafka == nil || groupID == "" {
		return
	}
	ctx := a.beginRequest("group")
	a.update(func(state *State) {
		state.GroupDetailLoading = true
		state.GroupDetailError = ""
	})
	go func() {
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		detail, err := a.kafka.LoadGroupDetail(requestCtx, connection, groupID)
		a.update(func(state *State) {
			if state.ActiveConnectionID != connection.ID || state.SelectedGroup != groupID {
				return
			}
			state.GroupDetailLoading = false
			if err != nil {
				state.GroupDetailError = err.Error()
				return
			}
			state.GroupDetail = detail
		})
	}()
}

func (a *App) ProduceMessage() {
	connection, ok := a.currentConnection()
	if !ok || a.kafka == nil {
		return
	}
	state := a.Snapshot()
	if strings.TrimSpace(state.Produce.Topic) == "" {
		a.setError("请输入 Topic")
		return
	}
	request := kafka.ProduceRequest{
		Topic:         strings.TrimSpace(state.Produce.Topic),
		AutoPartition: state.Produce.AutoPartition,
		Key:           []byte(state.Produce.Key),
		Value:         []byte(state.Produce.Value),
		Compression:   state.Produce.Compression,
	}
	var err error
	if !request.AutoPartition {
		request.Partition, err = parseNonNegativeInt32(state.Produce.Partition, "Partition")
		if err != nil {
			a.setError(err.Error())
			return
		}
	}
	if state.Produce.Headers != "" {
		request.Headers, err = parseHeaders(state.Produce.Headers)
		if err != nil {
			a.setError(err.Error())
			return
		}
	}
	if state.Produce.TimestampEnabled {
		if state.Produce.Timestamp.IsZero() {
			a.setError("消息时间不能为空")
			return
		}
		request.Timestamp = state.Produce.Timestamp
	}
	ctx := a.beginRequest("produce")
	a.update(func(state *State) {
		state.ProduceBusy = true
		state.ProduceError = ""
		state.Error = ""
	})
	go func() {
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		result, err := a.kafka.Produce(requestCtx, connection, request)
		a.update(func(state *State) {
			if state.ActiveConnectionID != connection.ID {
				return
			}
			state.ProduceBusy = false
			if err != nil {
				state.ProduceError = err.Error()
				return
			}
			state.LastProduce = &result
			state.Toast = fmt.Sprintf("消息已发送到 %s/%d，Offset %d", result.Topic, result.Partition, result.Offset)
		})
	}()
}

func (a *App) TestConnection(form ConnectionForm) {
	connection, err := a.connectionFromForm(form)
	if err != nil {
		a.update(func(state *State) {
			state.ConnectionTesting = false
			state.ConnectionTestText = err.Error()
		})
		return
	}
	if a.kafka == nil {
		a.update(func(state *State) {
			state.ConnectionTesting = false
			state.ConnectionTestText = "Kafka 服务尚未初始化"
		})
		return
	}
	credentials := kafka.ClientCredentials{
		KafkaPassword: form.Password,
		ProxyPassword: form.ProxyPassword,
	}
	testedFingerprint := connectionTransportFingerprint(connection, credentials)
	ctx := a.beginRequest("test-connection")
	a.update(func(state *State) {
		state.ConnectionTesting = true
		state.ConnectionTestText = "正在连接…"
		state.ConnectionTestedFingerprint = ""
		state.ConnectionHostKeyCandidate = ""
		state.ConnectionHostKeyFingerprint = ""
	})
	go func() {
		requestCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		err := a.kafka.TestConnection(requestCtx, connection, credentials)
		a.update(func(state *State) {
			if ctx.Err() != nil {
				return
			}
			state.ConnectionTesting = false
			if err != nil {
				var hostKeyErr *kafka.SSHHostKeyError
				if errors.As(err, &hostKeyErr) {
					state.ConnectionHostKeyCandidate = hostKeyErr.HostKey
					state.ConnectionHostKeyFingerprint = hostKeyErr.Fingerprint
					state.ConnectionTestText = "检测到 SSH 主机指纹，请核对后确认信任"
					return
				}
				state.ConnectionTestText = err.Error()
				return
			}
			state.ConnectionHostKeyCandidate = ""
			state.ConnectionHostKeyFingerprint = ""
			state.ConnectionTestedFingerprint = testedFingerprint
			state.ConnectionTestText = "连接成功"
			state.StatusText = "连接成功"
		})
	}()
}

func (a *App) StartConsumerSession() {
	connection, ok := a.currentConnection()
	if !ok || a.kafka == nil {
		return
	}
	state := a.Snapshot()
	if state.ConsumerSession != nil {
		a.setError("当前连接已有活动的消费会话")
		return
	}
	options := kafka.ConsumerOptions{
		GroupID:    strings.TrimSpace(state.Consumer.GroupID),
		Topic:      strings.TrimSpace(state.Consumer.Topic),
		FromStart:  state.Consumer.FromStart,
		AutoCommit: state.Consumer.AutoCommit,
	}
	if options.GroupID == "" || options.Topic == "" {
		a.setError("Consumer Group 和 Topic 不能为空")
		return
	}
	a.stopConsumer()
	ctx, cancel := context.WithCancel(context.Background())
	a.requestMu.Lock()
	a.consumerCancel = cancel
	a.requestMu.Unlock()
	a.update(func(state *State) {
		state.ConsumerBusy = true
		state.ConsumerError = ""
		state.ConsumerRecords = nil
		state.Error = ""
		state.SelectedGroup = options.GroupID
		state.GroupTab = 1
	})
	go func() {
		session, err := a.kafka.StartConsumer(ctx, connection, options)
		if err != nil {
			cancel()
			a.update(func(state *State) {
				if state.ActiveConnectionID != connection.ID {
					return
				}
				state.ConsumerBusy = false
				state.ConsumerError = err.Error()
			})
			return
		}
		a.update(func(state *State) {
			if state.ActiveConnectionID != connection.ID {
				session.Close()
				return
			}
			state.ConsumerSession = session
			state.ConsumerBusy = false
			state.ConsumerPaused = false
		})
		a.pollConsumer(ctx, connection, session)
	}()
}

func (a *App) pollConsumer(ctx context.Context, connection model.Connection, session *kafka.ConsumerSession) {
	for {
		if err := ctx.Err(); err != nil {
			session.Close()
			a.update(func(state *State) {
				if state.ConsumerSession == session {
					state.ConsumerSession = nil
				}
				state.ConsumerBusy = false
			})
			return
		}
		records, err := session.Poll(ctx, 100)
		if err != nil {
			if ctx.Err() == nil {
				a.update(func(state *State) {
					if state.ConsumerSession == session {
						state.ConsumerError = err.Error()
					}
				})
			}
			session.Close()
			a.update(func(state *State) {
				if state.ConsumerSession == session {
					state.ConsumerSession = nil
					state.ConsumerPaused = false
				}
			})
			return
		}
		if len(records) > 0 {
			a.update(func(state *State) {
				if state.ConsumerSession != session {
					return
				}
				state.ConsumerRecords = append(state.ConsumerRecords, records...)
				if len(state.ConsumerRecords) > 1000 {
					state.ConsumerRecords = append([]kafka.ConsumerRecord(nil), state.ConsumerRecords[len(state.ConsumerRecords)-1000:]...)
				}
			})
		}
	}
}

func (a *App) PauseConsumer() {
	session := a.Snapshot().ConsumerSession
	if session == nil || session.Paused() {
		return
	}
	session.Pause()
	a.update(func(state *State) { state.ConsumerPaused = true })
}

func (a *App) ResumeConsumer() {
	session := a.Snapshot().ConsumerSession
	if session == nil || !session.Paused() {
		return
	}
	session.Resume()
	a.update(func(state *State) { state.ConsumerPaused = false })
}

func (a *App) CommitConsumer() {
	session := a.Snapshot().ConsumerSession
	if session == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		err := session.Commit(ctx)
		a.update(func(state *State) {
			if err != nil {
				state.ConsumerError = err.Error()
				return
			}
			state.ConsumerError = ""
			state.Toast = "消费进度已提交"
		})
	}()
}

func (a *App) StopConsumer() {
	a.stopConsumer()
	a.update(func(state *State) {
		state.ConsumerBusy = false
		state.ConsumerPaused = false
		state.ConsumerSession = nil
	})
}

func (a *App) stopConsumer() {
	a.requestMu.Lock()
	if a.consumerCancel != nil {
		a.consumerCancel()
		a.consumerCancel = nil
	}
	a.requestMu.Unlock()
	a.mu.RLock()
	session := a.state.ConsumerSession
	a.mu.RUnlock()
	if session != nil {
		session.Close()
	}
}

func (a *App) Close() {
	a.requestMu.Lock()
	if a.closed {
		a.requestMu.Unlock()
		return
	}
	a.closed = true
	cancels := make([]context.CancelFunc, 0, len(a.requestCancels)+2)
	for _, cancel := range a.requestCancels {
		cancels = append(cancels, cancel)
	}
	a.requestCancels = make(map[string]context.CancelFunc)
	if a.consumerCancel != nil {
		cancels = append(cancels, a.consumerCancel)
		a.consumerCancel = nil
	}
	if a.autoRefreshCancel != nil {
		cancels = append(cancels, a.autoRefreshCancel)
		a.autoRefreshCancel = nil
	}
	a.requestMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	a.mu.RLock()
	session := a.state.ConsumerSession
	a.mu.RUnlock()
	if session != nil {
		session.Close()
	}
	if a.kafka != nil {
		a.kafka.Close()
	}
}

func parseHeaders(raw string) ([]kafka.Header, error) {
	headers := make([]kafka.Header, 0)
	for lineNumber, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		index := strings.IndexAny(line, ":=")
		if index <= 0 {
			return nil, fmt.Errorf("Header 第 %d 行格式应为 key: value", lineNumber+1)
		}
		key := strings.TrimSpace(line[:index])
		if key == "" {
			return nil, fmt.Errorf("Header 第 %d 行缺少 key", lineNumber+1)
		}
		headers = append(headers, kafka.Header{Key: key, Value: []byte(strings.TrimSpace(line[index+1:]))})
	}
	return headers, nil
}

func parseNonNegativeInt32(raw, name string) (int32, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s 必须是非负整数", name)
	}
	return int32(value), nil
}

func parsePartition(raw string) (int32, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err != nil || value < -1 {
		return 0, fmt.Errorf("Partition 必须为 -1 或非负整数")
	}
	return int32(value), nil
}

func parseNonNegativeInt64(raw, name string) (int64, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s 必须是非负整数", name)
	}
	return value, nil
}

func parsePositiveInt(raw, name string, maximum int) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s 必须是正整数", name)
	}
	if value > maximum {
		value = maximum
	}
	return value, nil
}

func topicNames(topics []kafka.TopicSummary) []string {
	names := make([]string, 0, len(topics))
	for _, topic := range topics {
		names = append(names, topic.Name)
	}
	return names
}

func keepKnownValue(value string, options []string) string {
	for _, option := range options {
		if option == value {
			return value
		}
	}
	if len(options) > 0 {
		return options[0]
	}
	return ""
}
