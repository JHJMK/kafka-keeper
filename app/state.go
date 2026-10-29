package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/egoist/mygo"

	"github.com/jjk/kafka-keeper/internal/kafka"
	"github.com/jjk/kafka-keeper/internal/model"
	"github.com/jjk/kafka-keeper/internal/store"
)

type Page string

const (
	PageOverview Page = "overview"
	PageTopics   Page = "topics"
	PageGroups   Page = "groups"
	PageSend     Page = "send"
)

type CredentialStore interface {
	Set(id, password string) error
	Get(id string) (string, bool, error)
	Delete(id string) error
	SessionOnly(id string) bool
	SetProxyPassword(id, password string) error
	GetProxyPassword(id string) (string, bool, error)
	DeleteProxyPassword(id string) error
	ProxySessionOnly(id string) bool
}

type KafkaAPI interface {
	TestConnection(context.Context, model.Connection, kafka.ClientCredentials) error
	ForgetConnection(string)
	Close()
	ListTopics(context.Context, model.Connection) ([]kafka.TopicSummary, error)
	LoadOverview(context.Context, model.Connection) (kafka.Overview, error)
	LoadTopicDetail(context.Context, model.Connection, string) (kafka.TopicDetail, error)
	ListGroups(context.Context, model.Connection) ([]kafka.GroupSummary, error)
	LoadGroupDetail(context.Context, model.Connection, string) (kafka.GroupDetail, error)
	BrowseMessages(context.Context, model.Connection, kafka.BrowseRequest) ([]kafka.Message, error)
	SearchMessages(context.Context, model.Connection, kafka.SearchRequest) (kafka.SearchResult, error)
	Produce(context.Context, model.Connection, kafka.ProduceRequest) (kafka.ProduceResult, error)
	StartConsumer(context.Context, model.Connection, kafka.ConsumerOptions) (*kafka.ConsumerSession, error)
}

type Dependencies struct {
	Connections store.ConnectionStore
	Credentials CredentialStore
	Kafka       KafkaAPI
}

type ConnectionForm struct {
	ID            string
	Name          string
	Servers       string
	ClientID      string
	TLSEnabled    bool
	CAFile        string
	SASLMechanism string
	Username      string
	Password      string
	ProxyEnabled  bool
	ProxyAddress  string
	ProxyPort     string
	ProxyUsername string
	ProxyPassword string
	ProxyHostKey  string
}

type BrowseForm struct {
	Partition     string
	Mode          string
	Offset        string
	Start         time.Time
	End           time.Time
	Limit         string
	AllPartitions bool
}

type SearchForm struct {
	Partition    string
	Start        time.Time
	StartEnabled bool
	End          time.Time
	EndEnabled   bool
	OffsetStart  string
	OffsetEnd    string
	Key          string
	KeyContains  bool
	HeaderKey    string
	HeaderValue  string
	MaxScan      string
	MaxResults   string
}

type ProduceForm struct {
	Topic            string
	Partition        string
	AutoPartition    bool
	Key              string
	Value            string
	Headers          string
	Compression      string
	Timestamp        time.Time
	TimestampEnabled bool
}

type ConsumerForm struct {
	GroupID    string
	Topic      string
	FromStart  bool
	AutoCommit bool
}

type State struct {
	Page               Page
	Connections        []model.Connection
	ActiveConnectionID string
	StatusText         string
	Toast              string
	ToastUntil         time.Time
	Error              string

	ConnectionDialog             bool
	DeleteDialog                 bool
	DeleteConnectionID           string
	ConnectionForm               ConnectionForm
	ConnectionTestText           string
	ConnectionTesting            bool
	ConnectionTestedFingerprint  string
	ConnectionHostKeyCandidate   string
	ConnectionHostKeyFingerprint string

	Overview        kafka.Overview
	OverviewReady   bool
	OverviewLoading bool
	OverviewError   string

	Topics        []kafka.TopicSummary
	TopicsLoading bool
	TopicsError   string
	TopicFilter   string

	SelectedTopic      string
	TopicDetail        kafka.TopicDetail
	TopicDetailLoading bool
	TopicDetailError   string
	TopicTab           int
	Browse             BrowseForm
	BrowseResults      []kafka.Message
	BrowseLoading      bool
	BrowseError        string
	BrowseLoaded       bool
	SelectedMessage    int
	Search             SearchForm
	SearchResults      []kafka.Message
	SearchScanned      int
	SearchTruncated    bool
	SearchLoading      bool
	SearchError        string
	SearchLoaded       bool

	Groups           []kafka.GroupSummary
	GroupsLoading    bool
	GroupsError      string
	GroupFilter      string
	ActiveGroupsOnly bool

	RefreshSeconds     string
	AutoRefreshEnabled bool
	AutoRefreshPage    Page

	SelectedGroup      string
	GroupDetail        kafka.GroupDetail
	GroupDetailLoading bool
	GroupDetailError   string
	GroupTab           int

	Produce      ProduceForm
	ProduceBusy  bool
	ProduceError string
	LastProduce  *kafka.ProduceResult

	Consumer        ConsumerForm
	ConsumerSession *kafka.ConsumerSession
	ConsumerRecords []kafka.ConsumerRecord
	ConsumerBusy    bool
	ConsumerPaused  bool
	ConsumerError   string
}

type App struct {
	mu         sync.RWMutex
	state      State
	win        *mygo.Window
	chooseFile func(mygo.OpenDialogOptions) ([]string, error)

	connections store.ConnectionStore
	credentials CredentialStore
	kafka       KafkaAPI

	requestMu         sync.Mutex
	requestCancels    map[string]context.CancelFunc
	consumerCancel    context.CancelFunc
	autoRefreshCancel context.CancelFunc
	closed            bool
}

func New() *App {
	return &App{
		state: State{
			Page:           PageOverview,
			StatusText:     "尚未连接 Kafka",
			TopicTab:       0,
			RefreshSeconds: "30",
			Browse: BrowseForm{
				Partition:     "0",
				Mode:          string(kafka.BrowseByOffset),
				Offset:        "0",
				Limit:         "100",
				AllPartitions: true,
			},
			Search: SearchForm{
				Partition:  "-1",
				MaxScan:    "5000",
				MaxResults: "100",
			},
			Produce: ProduceForm{
				AutoPartition: true,
				Compression:   "none",
			},
			Consumer: ConsumerForm{FromStart: true},
		},
		requestCancels: make(map[string]context.CancelFunc),
		chooseFile:     mygo.Dialog.Open,
	}
}

func NewWithDependencies(dependencies Dependencies) *App {
	a := New()
	a.connections = dependencies.Connections
	a.credentials = dependencies.Credentials
	a.kafka = dependencies.Kafka
	if err := a.reloadConnections(); err != nil {
		a.setError(err.Error())
	}
	return a
}

func (a *App) Snapshot() State {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.state
}

func (a *App) SetWindow(win *mygo.Window) {
	a.mu.Lock()
	a.win = win
	a.mu.Unlock()
}

func (a *App) update(fn func(*State)) {
	a.mu.Lock()
	fn(&a.state)
	win := a.win
	a.mu.Unlock()
	if win != nil && !win.IsDestroyed() {
		win.Update(func() {})
	}
}

func (a *App) setError(message string) {
	a.update(func(state *State) { state.Error = message })
}

func (a *App) chooseCAFile() (string, error) {
	chooseFile := a.chooseFile
	if chooseFile == nil {
		chooseFile = mygo.Dialog.Open
	}
	a.mu.RLock()
	win := a.win
	a.mu.RUnlock()
	paths, err := chooseFile(mygo.OpenDialogOptions{
		Parent:      win,
		Title:       "选择 CA 证书",
		ButtonLabel: "选择",
		Message:     "选择用于验证 Kafka Broker 的 CA 证书文件",
		Filters: []mygo.FileFilter{{
			Name:       "证书文件",
			Extensions: []string{"pem", "crt", "cer"},
		}},
	})
	if err != nil || len(paths) == 0 {
		return "", err
	}
	return filepath.Abs(paths[0])
}

func (a *App) clearMessages() {
	a.update(func(state *State) {
		state.Error = ""
		state.Toast = ""
		state.ToastUntil = time.Time{}
	})
}

func (a *App) Navigate(page Page) {
	if a.Snapshot().Page != page {
		a.stopAutoRefresh()
	}
	a.update(func(state *State) { state.Page = page })
}

func (a *App) ShowToast(message string) {
	a.update(func(state *State) {
		state.Toast = message
		state.ToastUntil = time.Time{}
		state.Error = ""
	})
}

func (a *App) ClearToast() {
	a.update(func(state *State) {
		state.Toast = ""
		state.ToastUntil = time.Time{}
	})
}

func (a *App) ActiveConnection() (model.Connection, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, connection := range a.state.Connections {
		if connection.ID == a.state.ActiveConnectionID {
			return connection, true
		}
	}
	return model.Connection{}, false
}

func (a *App) SelectConnection(id string) {
	a.stopAutoRefresh()
	a.stopConsumer()
	a.cancelRequest("metadata")
	a.cancelRequest("topic")
	a.cancelRequest("browse")
	a.cancelRequest("search")
	a.cancelRequest("group")
	a.update(func(state *State) {
		state.ActiveConnectionID = id
		state.Overview = kafka.Overview{}
		state.OverviewReady = false
		state.OverviewError = ""
		state.Topics = nil
		state.TopicsError = ""
		state.SelectedTopic = ""
		state.TopicDetail = kafka.TopicDetail{}
		state.TopicDetailError = ""
		state.BrowseResults = nil
		state.BrowseError = ""
		state.SearchResults = nil
		state.SearchError = ""
		state.Groups = nil
		state.GroupsError = ""
		state.SelectedGroup = ""
		state.GroupDetail = kafka.GroupDetail{}
		state.GroupDetailError = ""
		state.ConsumerRecords = nil
		state.ConsumerError = ""
		state.StatusText = "正在读取集群信息"
	})
	a.RefreshAll()
}

func (a *App) OpenNewConnection() {
	a.update(func(state *State) {
		state.ConnectionDialog = true
		state.ConnectionForm = ConnectionForm{SASLMechanism: model.SASLNone, ProxyPort: "22"}
		state.ConnectionTestText = ""
		state.ConnectionTestedFingerprint = ""
		state.ConnectionHostKeyCandidate = ""
		state.ConnectionHostKeyFingerprint = ""
	})
}

func (a *App) CloseConnectionDialog() {
	a.update(func(state *State) {
		state.ConnectionDialog = false
		state.ConnectionTesting = false
		state.ConnectionTestText = ""
		state.ConnectionTestedFingerprint = ""
		state.ConnectionHostKeyCandidate = ""
		state.ConnectionHostKeyFingerprint = ""
	})
}

func (a *App) SaveConnection(form ConnectionForm) error {
	if a.connections == nil {
		return fmt.Errorf("连接存储尚未初始化")
	}
	if a.credentials == nil {
		return fmt.Errorf("凭据存储尚未初始化")
	}
	connection, err := a.connectionFromForm(form)
	if err != nil {
		return err
	}
	if connection.Proxy.Enabled && strings.TrimSpace(connection.Proxy.HostKey) == "" {
		return fmt.Errorf("请先测试连接并确认 SSH 主机指纹")
	}
	if connection.Proxy.Enabled {
		credentials := kafka.ClientCredentials{
			KafkaPassword: form.Password,
			ProxyPassword: form.ProxyPassword,
		}
		fingerprint := connectionTransportFingerprint(connection, credentials)
		testedFingerprint := a.Snapshot().ConnectionTestedFingerprint
		if testedFingerprint != fingerprint {
			storedFingerprint, found, err := a.storedProxyFingerprint(connection.ID)
			if err != nil {
				return err
			}
			if !found || storedFingerprint != fingerprint {
				return fmt.Errorf("SSH 代理配置已变更，请重新测试连接并确认主机指纹")
			}
		}
	}
	if connection.TLS.Enabled && connection.TLS.CAFile != "" {
		connection.TLS.CAFile, err = copyCAFile(connection.TLS.CAFile, connection.ID)
		if err != nil {
			return fmt.Errorf("复制 TLS CA 证书: %w", err)
		}
	}
	if err := a.connections.Save(connection); err != nil {
		return err
	}
	if strings.TrimSpace(form.Password) != "" {
		if err := a.credentials.Set(connection.ID, form.Password); err != nil {
			return fmt.Errorf("保存 Kafka 密码: %w", err)
		}
	}
	if connection.Proxy.Enabled {
		if strings.TrimSpace(form.ProxyPassword) != "" {
			if err := a.credentials.SetProxyPassword(connection.ID, form.ProxyPassword); err != nil {
				return fmt.Errorf("保存 SSH 代理密码: %w", err)
			}
		}
	} else if err := a.credentials.DeleteProxyPassword(connection.ID); err != nil {
		return fmt.Errorf("删除 SSH 代理密码: %w", err)
	}
	if err := a.reloadConnections(); err != nil {
		return err
	}
	a.update(func(state *State) {
		state.ActiveConnectionID = connection.ID
		state.ConnectionDialog = false
		state.ConnectionForm = ConnectionForm{}
		state.ConnectionTestedFingerprint = ""
		state.ConnectionHostKeyCandidate = ""
		state.ConnectionHostKeyFingerprint = ""
		state.StatusText = "连接已保存"
	})
	return nil
}

func (a *App) DeleteConnection(connectionID string) error {
	snapshot := a.Snapshot()
	connection, ok := connectionByID(snapshot.Connections, connectionID)
	if !ok {
		return fmt.Errorf("连接不存在")
	}

	deletingActive := snapshot.ActiveConnectionID == connectionID
	nextActiveID := snapshot.ActiveConnectionID
	if deletingActive {
		nextActiveID = ""
		for i, item := range snapshot.Connections {
			if item.ID == connectionID && i+1 < len(snapshot.Connections) {
				nextActiveID = snapshot.Connections[i+1].ID
				break
			}
		}
	}

	if a.kafka != nil {
		a.kafka.ForgetConnection(connection.ID)
	}
	if deletingActive {
		a.stopConsumer()
	}
	if a.credentials != nil {
		if err := a.credentials.Delete(connection.ID); err != nil {
			return fmt.Errorf("删除 Kafka 凭据: %w", err)
		}
		if err := a.credentials.DeleteProxyPassword(connection.ID); err != nil {
			return fmt.Errorf("删除 SSH 代理凭据: %w", err)
		}
	}
	if a.connections == nil {
		return fmt.Errorf("连接存储尚未初始化")
	}
	if err := a.connections.Delete(connection.ID); err != nil {
		return err
	}
	removeManagedCAFile(connection)
	if err := a.reloadConnections(); err != nil {
		return err
	}
	a.update(func(state *State) {
		if deletingActive {
			state.ActiveConnectionID = nextActiveID
		}
		state.ConnectionDialog = false
		state.DeleteDialog = false
		state.DeleteConnectionID = ""
		state.StatusText = "连接已删除"
	})
	return nil
}

func (a *App) reloadConnections() error {
	if a.connections == nil {
		return nil
	}
	connections, err := a.connections.List()
	if err != nil {
		return err
	}
	a.update(func(state *State) {
		state.Connections = connections
		active := ""
		for _, connection := range connections {
			if connection.ID == state.ActiveConnectionID {
				active = connection.ID
				break
			}
		}
		if active == "" && len(connections) > 0 {
			active = connections[0].ID
		}
		state.ActiveConnectionID = active
		switch {
		case len(connections) == 0:
			state.StatusText = "尚未连接 Kafka"
		case active != "":
			state.StatusText = "已选择连接"
		}
	})
	return nil
}

func (a *App) connectionFromForm(form ConnectionForm) (model.Connection, error) {
	proxyPort := 22
	if value := strings.TrimSpace(form.ProxyPort); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 65535 {
			return model.Connection{}, fmt.Errorf("SSH 端口必须是 1 到 65535 之间的整数")
		}
		proxyPort = parsed
	}
	now := time.Now()
	connection := model.Connection{
		ID:               strings.TrimSpace(form.ID),
		Name:             strings.TrimSpace(form.Name),
		BootstrapServers: model.ParseServers(form.Servers),
		ClientID:         strings.TrimSpace(form.ClientID),
		TLS: model.TLSConfig{
			Enabled: form.TLSEnabled,
			CAFile:  strings.TrimSpace(form.CAFile),
		},
		SASL: model.SASLConfig{
			Mechanism: form.SASLMechanism,
			Username:  strings.TrimSpace(form.Username),
		},
		Proxy: model.SSHProxyConfig{
			Enabled:  form.ProxyEnabled,
			Address:  strings.TrimSpace(form.ProxyAddress),
			Port:     proxyPort,
			Username: strings.TrimSpace(form.ProxyUsername),
			HostKey:  strings.TrimSpace(form.ProxyHostKey),
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	if connection.ID != "" {
		for _, existing := range a.Snapshot().Connections {
			if existing.ID == connection.ID {
				connection.CreatedAt = existing.CreatedAt
				break
			}
		}
	} else {
		connection.ID = newConnectionID()
	}
	if err := connection.Validate(); err != nil {
		return model.Connection{}, err
	}
	return connection, nil
}

func formFromConnection(connection model.Connection) ConnectionForm {
	return ConnectionForm{
		ID:            connection.ID,
		Name:          connection.Name,
		Servers:       connection.ServersText(),
		ClientID:      connection.ClientID,
		TLSEnabled:    connection.TLS.Enabled,
		CAFile:        connection.TLS.CAFile,
		SASLMechanism: connection.SASL.Mechanism,
		Username:      connection.SASL.Username,
		ProxyEnabled:  connection.Proxy.Enabled,
		ProxyAddress:  connection.Proxy.Address,
		ProxyPort:     strconv.Itoa(connection.Proxy.PortOrDefault()),
		ProxyUsername: connection.Proxy.Username,
		ProxyHostKey:  connection.Proxy.HostKey,
	}
}

func (a *App) connectionFormForEdit(connectionID string) (ConnectionForm, error) {
	connection, ok := connectionByID(a.Snapshot().Connections, connectionID)
	if !ok {
		return ConnectionForm{}, fmt.Errorf("连接不存在")
	}
	form := formFromConnection(connection)
	if a.credentials == nil {
		return form, nil
	}
	password, found, err := a.credentials.Get(connection.ID)
	if err != nil {
		return form, fmt.Errorf("读取 Kafka 密码: %w", err)
	}
	if found {
		form.Password = password
	}
	if connection.Proxy.Enabled {
		proxyPassword, found, err := a.credentials.GetProxyPassword(connection.ID)
		if err != nil {
			return form, fmt.Errorf("读取 SSH 代理密码: %w", err)
		}
		if found {
			form.ProxyPassword = proxyPassword
		}
	}
	return form, nil
}

func (a *App) storedProxyFingerprint(connectionID string) (string, bool, error) {
	connection, ok := connectionByID(a.Snapshot().Connections, connectionID)
	if !ok || !connection.Proxy.Enabled || a.credentials == nil {
		return "", false, nil
	}
	kafkaPassword, _, err := a.credentials.Get(connection.ID)
	if err != nil {
		return "", false, fmt.Errorf("读取 Kafka 密码: %w", err)
	}
	proxyPassword, _, err := a.credentials.GetProxyPassword(connection.ID)
	if err != nil {
		return "", false, fmt.Errorf("读取 SSH 代理密码: %w", err)
	}
	return connectionTransportFingerprint(connection, kafka.ClientCredentials{
		KafkaPassword: kafkaPassword,
		ProxyPassword: proxyPassword,
	}), true, nil
}

func connectionTransportFingerprint(connection model.Connection, credentials kafka.ClientCredentials) string {
	connection.ID = ""
	connection.Name = ""
	connection.ClientID = ""
	connection.CreatedAt = time.Time{}
	connection.UpdatedAt = time.Time{}
	if connection.Proxy.Enabled {
		connection.Proxy.Port = connection.Proxy.PortOrDefault()
	}
	kafkaPasswordHash := sha256.Sum256([]byte(credentials.KafkaPassword))
	proxyPasswordHash := sha256.Sum256([]byte(credentials.ProxyPassword))
	payload, _ := json.Marshal(struct {
		Connection        model.Connection `json:"connection"`
		KafkaPasswordHash string           `json:"kafkaPasswordHash"`
		ProxyPasswordHash string           `json:"proxyPasswordHash"`
	}{
		Connection:        connection,
		KafkaPasswordHash: hex.EncodeToString(kafkaPasswordHash[:]),
		ProxyPasswordHash: hex.EncodeToString(proxyPasswordHash[:]),
	})
	fingerprint := sha256.Sum256(payload)
	return hex.EncodeToString(fingerprint[:])
}

func newConnectionID() string {
	var value [8]byte
	if _, err := rand.Read(value[:]); err == nil {
		return hex.EncodeToString(value[:])
	}
	return fmt.Sprintf("connection-%d", time.Now().UnixNano())
}

func removeManagedCAFile(connection model.Connection) {
	if strings.TrimSpace(connection.TLS.CAFile) == "" {
		return
	}
	dataDir, err := mygo.App.Path(mygo.PathUserData)
	if err != nil {
		return
	}
	managedPath := filepath.Join(dataDir, "ca-certificates", connection.ID+".pem")
	if filepath.Clean(connection.TLS.CAFile) != filepath.Clean(managedPath) {
		return
	}
	_ = os.Remove(managedPath)
}

func copyCAFile(source, connectionID string) (string, error) {
	dataDir, err := mygo.App.Path(mygo.PathUserData)
	if err != nil {
		return "", err
	}
	certificateDir := filepath.Join(dataDir, "ca-certificates")
	if err := os.MkdirAll(certificateDir, 0o700); err != nil {
		return "", err
	}
	data, err := os.ReadFile(source)
	if err != nil {
		return "", err
	}
	destination := filepath.Join(certificateDir, connectionID+".pem")
	if err := os.WriteFile(destination, data, 0o600); err != nil {
		return "", err
	}
	return destination, nil
}
