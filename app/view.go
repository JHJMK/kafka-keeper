package app

import (
	"fmt"
	"strings"
	"time"

	"github.com/egoist/mygo/ui"

	"github.com/jjk/kafka-keeper/internal/kafka"
	"github.com/jjk/kafka-keeper/internal/model"
	uikit "github.com/jjk/kafka-keeper/internal/ui"
)

const (
	appToastID       = "app.toast"
	appToastLifetime = 4 * time.Second
)

func (a *App) View() func(c *ui.Context) {
	return func(c *ui.Context) {
		theme := uikit.Theme()
		c.SetTheme(theme)
		c.Root().Background(theme.Background)

		view := a.Snapshot()
		now := c.Now()
		switch {
		case view.Toast == "":
			view.ToastUntil = time.Time{}
		case view.ToastUntil.IsZero():
			view.ToastUntil = now.Add(appToastLifetime)
		case !now.Before(view.ToastUntil):
			view.Toast = ""
			view.ToastUntil = time.Time{}
		}

		ui.Row(c).Fill().Children(func() {
			a.sidebar(c, &view)
			a.mainContent(c, &view)
		})
		a.toast(c, &view)
		a.connectionDialogs(c, &view)
		if view.Toast != "" {
			c.After(view.ToastUntil.Sub(now))
		}
		a.commitView(&view)
	}
}

func (a *App) toast(c *ui.Context, view *State) {
	if view.Toast == "" {
		return
	}
	theme := c.Theme()
	ui.Row(c).Absolute().Left(0).Right(0).Bottom(16).Justify(ui.Center).PassThrough().Children(func() {
		ui.Row(c).Key(appToastID).Padding(10, 16).Radius(12).
			Background(theme.Text).TextColor(theme.Surface).
			Shadow(0, 6, 20, 0, ui.RGBA(36, 69, 90, 0.24)).Children(func() {
			ui.Text(c, view.Toast).FontSize(12).Bold().TextColor(theme.Surface)
		})
	})
}

func (a *App) commitView(view *State) {
	a.mu.Lock()
	a.state.ConnectionDialog = view.ConnectionDialog
	a.state.DeleteDialog = view.DeleteDialog
	a.state.DeleteConnectionID = view.DeleteConnectionID
	a.state.ConnectionForm = view.ConnectionForm
	a.state.ConnectionTesting = view.ConnectionTesting
	a.state.ConnectionTestText = view.ConnectionTestText
	a.state.ConnectionTestedFingerprint = view.ConnectionTestedFingerprint
	a.state.ConnectionHostKeyCandidate = view.ConnectionHostKeyCandidate
	a.state.ConnectionHostKeyFingerprint = view.ConnectionHostKeyFingerprint
	a.state.TopicFilter = view.TopicFilter
	a.state.SelectedMessage = view.SelectedMessage
	a.state.TopicTab = view.TopicTab
	a.state.Browse = view.Browse
	a.state.Search = view.Search
	a.state.GroupFilter = view.GroupFilter
	a.state.ActiveGroupsOnly = view.ActiveGroupsOnly
	a.state.RefreshSeconds = view.RefreshSeconds
	a.state.GroupTab = view.GroupTab
	a.state.Produce = view.Produce
	a.state.Consumer = view.Consumer
	a.state.Toast = view.Toast
	a.state.ToastUntil = view.ToastUntil
	a.mu.Unlock()
}

func (a *App) sidebar(c *ui.Context, view *State) {
	theme := c.Theme()
	ui.Column(c).Width(260).FillHeight().Padding(18).Gap(14).
		Gradient(uikit.Hex(uikit.SidebarTop), uikit.Hex(uikit.SidebarBottom), 180).
		Children(func() {
			ui.Row(c).Gap(10).AlignItems(ui.Center).Children(func() {
				ui.Column(c).Size(38, 38).Radius(12).Center().Background(theme.Accent).Children(func() {
					ui.Text(c, "K").FontSize(20).Bold().TextColor(theme.AccentText)
				})
				ui.Column(c).Gap(1).Children(func() {
					ui.Text(c, "Kafka Keeper").FontSize(19).Bold()
					ui.Text(c, "Native Kafka Client").FontSize(11).TextColor(theme.TextMuted)
				})
			})

			ui.Divider(c).MarginY(2)

			ui.Column(c).Grow(5).FillWidth().Label("Kafka 连接区域").Gap(10).Children(func() {
				ui.Row(c).AlignItems(ui.Center).Children(func() {
					ui.Text(c, "KAFKA 连接").FontSize(11).Bold().TextColor(theme.TextMuted)
					ui.Spacer(c).Grow(1)
					if ui.PrimaryButton(c, "添加连接").Clicked() {
						view.ConnectionDialog = true
						view.ConnectionForm = ConnectionForm{SASLMechanism: model.SASLNone, ProxyPort: "22"}
						view.ConnectionTestText = ""
						view.ConnectionHostKeyCandidate = ""
						view.ConnectionHostKeyFingerprint = ""
					}
				})

				if len(view.Connections) == 0 {
					ui.Column(c).Padding(12).Gap(4).Radius(12).Background(theme.Surface).Border(1, theme.Border).Children(func() {
						ui.Text(c, "还没有 Kafka 连接").Bold()
						ui.Text(c, "添加连接后即可浏览集群。").FontSize(11).TextColor(theme.TextMuted)
					})
				} else {
					ui.Scroll(c).Grow(1).FillWidth().Label("Kafka 连接列表").Gap(7).Children(func() {
						for _, connection := range view.Connections {
							connection := connection
							selected := connection.ID == view.ActiveConnectionID
							row := ui.Row(c.Key(connection.ID)).FillWidth().Padding(9, 10).Radius(10).Gap(8).
								AlignItems(ui.Center).Background(theme.Surface)
							if selected {
								row.Border(1, theme.Accent).Background(theme.Selection)
							} else {
								row.Border(1, theme.Border)
							}
							row.Children(func() {
								ui.Column(c).Size(8, 8).Radius(4).Background(theme.Accent)
								ui.Column(c).Grow(1).Gap(1).Children(func() {
									ui.Text(c, connection.Name).FontSize(13).Bold().SingleLine()
									ui.Text(c, connection.ServersText()).FontSize(10).TextColor(theme.TextMuted).SingleLine()
								})
								more := ui.ButtonBase(c).
									Key("connection-menu-"+connection.ID).
									Size(28, 28).
									Radius(8).
									Center().
									Label("连接操作：" + connection.Name)
								more.Children(func() {
									ui.Text(c, "...").FontSize(15).Bold().TextColor(theme.TextMuted)
								})
								more.PopoverMenuButton(func(m *ui.Menu) {
									if m.Item("编辑连接").Chosen() {
										view.ConnectionDialog = true
										view.ConnectionTestText = ""
										view.ConnectionTestedFingerprint = ""
										view.ConnectionHostKeyCandidate = ""
										view.ConnectionHostKeyFingerprint = ""
										form, err := a.connectionFormForEdit(connection.ID)
										view.ConnectionForm = form
										if err != nil {
											view.ConnectionTestText = err.Error()
										}
									}
									if m.Item("删除连接").Chosen() {
										view.DeleteConnectionID = connection.ID
										view.DeleteDialog = true
									}
								})
							})
							if row.Clicked() && !selected {
								view.ActiveConnectionID = connection.ID
								a.SelectConnection(connection.ID)
							}
						}
					})
				}
			})

			ui.Column(c).Grow(5).FillWidth().Label("工作区区域").Gap(8).Children(func() {
				ui.Text(c, "工作区").FontSize(11).Bold().TextColor(theme.TextMuted)
				ui.Column(c).FillWidth().Gap(8).Children(func() {
					a.sidebarNav(c, view, "概览", PageOverview)
					a.sidebarNav(c, view, "Topics", PageTopics)
					a.sidebarNav(c, view, "Consumer Groups", PageGroups)
					a.sidebarNav(c, view, "发送消息", PageSend)
				})
			})

			ui.Divider(c)
			ui.Column(c).Gap(4).Children(func() {
				ui.Text(c, "当前连接").FontSize(11).TextColor(theme.TextMuted)
				connection, ok := connectionByID(view.Connections, view.ActiveConnectionID)
				if !ok {
					ui.Text(c, "未选择").FontSize(13).Bold()
					return
				}
				ui.Text(c, connection.Name).FontSize(13).Bold().SingleLine()
				ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
					ui.Column(c).Size(7, 7).Radius(4).Background(theme.Success)
					ui.Text(c, view.StatusText).FontSize(10).TextColor(theme.TextMuted).SingleLine()
				})
			})
		})
}

func (a *App) sidebarNav(c *ui.Context, view *State, label string, page Page) {
	theme := c.Theme()
	selected := view.Page == page
	row := ui.Row(c.Key(page)).FillWidth().Height(40).PaddingX(12).Radius(10).AlignItems(ui.Center).Gap(10)
	if selected {
		row.Background(theme.Accent)
	} else {
		row.Background(theme.Surface)
	}
	row.Children(func() {
		ui.Column(c).Size(6, 6).Radius(3).Background(theme.Accent)
		if selected {
			ui.Text(c, label).Bold().TextColor(theme.AccentText)
		} else {
			ui.Text(c, label).TextColor(theme.Text)
		}
	})
	if row.Clicked() && !selected {
		a.Navigate(page)
	}
}

func (a *App) mainContent(c *ui.Context, view *State) {
	theme := c.Theme()
	title, subtitle := pageHeading(view.Page)
	ui.Column(c).Grow(1).FillHeight().Padding(24, 24, 22, 24).Gap(14).Children(func() {
		ui.Row(c).FillWidth().AlignItems(ui.Center).Children(func() {
			ui.Column(c).Gap(2).Children(func() {
				ui.Text(c, title).FontSize(26).Bold()
				ui.Text(c, subtitle).FontSize(12).TextColor(theme.TextMuted)
			})
			ui.Spacer(c).Grow(1)
			if view.Error != "" {
				if ui.Button(c, "清除错误").Clicked() {
					view.Error = ""
				}
			}
			if view.ActiveConnectionID != "" && view.Page != PageSend {
				ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
					ui.Text(c, "刷新间隔").FontSize(10).TextColor(theme.TextMuted)
					ui.TextInput(c.Key("自动刷新间隔秒数"), &view.RefreshSeconds).Label("自动刷新间隔秒数").Placeholder("30").Width(64)
					ui.Text(c, "秒").FontSize(10).TextColor(theme.TextMuted)
					if ui.Button(c, autoRefreshButtonLabel(*view)).Clicked() {
						if view.AutoRefreshEnabled {
							a.StopAutoRefresh()
						} else {
							a.StartAutoRefresh(view.RefreshSeconds)
						}
					}
					if ui.Button(c, "刷新").Clicked() {
						a.refreshPage(view.Page)
					}
				})
			}
		})

		if view.Error != "" {
			ui.Row(c).FillWidth().Padding(10, 12).Radius(10).Background(ui.RGBA(216, 91, 103, 0.10)).
				Border(1, ui.RGBA(216, 91, 103, 0.30)).Gap(10).AlignItems(ui.Center).Children(func() {
				ui.Text(c, view.Error).FontSize(12).TextColor(theme.Danger).Grow(1)
			})
		}

		if view.ActiveConnectionID == "" {
			a.emptyConnection(c)
			return
		}

		switch view.Page {
		case PageOverview:
			a.overviewPage(c, view)
		case PageTopics:
			a.topicsPage(c, view)
		case PageGroups:
			a.groupsPage(c, view)
		case PageSend:
			a.sendPage(c, view)
		}
	})
}

func (a *App) emptyConnection(c *ui.Context) {
	theme := c.Theme()
	ui.Column(c).Grow(1).FillWidth().Center().Children(func() {
		ui.Column(c).Width(420).Padding(28).Gap(10).Radius(uikit.PanelRadius).Background(theme.Surface).
			Border(1, theme.Border).Center().Children(func() {
			ui.Text(c, "还没有 Kafka 连接").FontSize(20).Bold()
			ui.Text(c, "先添加 Bootstrap Servers、TLS 和 SASL 信息，再开始管理 Topic 与 Consumer Group。").
				FontSize(12).TextColor(theme.TextMuted).TextAlign(ui.Center)
		})
	})
}

func pageHeading(page Page) (string, string) {
	switch page {
	case PageTopics:
		return "Topics", "浏览分区、消息与 Topic 配置"
	case PageGroups:
		return "Consumer Groups", "检查成员、Offset、Lag 与受控消费"
	case PageSend:
		return "发送消息", "使用 franz-go 向 Kafka 生产消息"
	default:
		return "集群概览", "连接状态、Broker 与消费积压"
	}
}

func connectionByID(connections []model.Connection, id string) (model.Connection, bool) {
	for _, connection := range connections {
		if connection.ID == id {
			return connection, true
		}
	}
	return model.Connection{}, false
}

func (a *App) connectionDialogs(c *ui.Context, view *State) {
	theme := c.Theme()
	ui.Modal(c, &view.ConnectionDialog, func() {
		title := "新建 Kafka 连接"
		if view.ConnectionForm.ID != "" {
			title = "编辑 Kafka 连接"
		}
		ui.Column(c).Width(650).Gap(14).Children(func() {
			ui.Text(c, title).FontSize(20).Bold()
			ui.Text(c, "密码只保存到系统凭据存储；不可用时会退化为本次会话有效。").
				FontSize(11).TextColor(theme.TextMuted)
			ui.Scroll(c).FillWidth().MaxHeight(620).Children(func() {
				ui.Column(c).FillWidth().Children(func() {
					ui.Form(c, func() {
						ui.Field(c, "连接名称", func() {
							ui.TextInput(c, &view.ConnectionForm.Name).Placeholder("例如：本地开发集群")
						})
						ui.Field(c, "Bootstrap Servers", func() {
							ui.TextInput(c, &view.ConnectionForm.Servers).Placeholder("localhost:9092, kafka-2:9092")
						})
						ui.Field(c, "Client ID", func() {
							ui.TextInput(c, &view.ConnectionForm.ClientID).Placeholder("kafka-keeper")
						})
						ui.Field(c, "SASL 机制", func() {
							label := saslMechanismLabel(view.ConnectionForm.SASLMechanism)
							ui.Select(c, &label, []string{"不使用", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"})
							view.ConnectionForm.SASLMechanism = saslMechanismValue(label)
						})
						if view.ConnectionForm.SASLMechanism != model.SASLNone {
							ui.Field(c, "SASL 用户名", func() {
								ui.TextInput(c, &view.ConnectionForm.Username)
							})
							ui.Field(c, "SASL 密码", func() {
								ui.TextInput(c, &view.ConnectionForm.Password).Label("SASL 密码输入框")
							})
						}
						ui.Field(c, "TLS", func() {
							ui.Switch(c, &view.ConnectionForm.TLSEnabled).Label("启用 TLS 1.2+")
						})
						if view.ConnectionForm.TLSEnabled {
							ui.Field(c, "CA 证书文件", func() {
								ui.Row(c).FillWidth().Gap(8).AlignItems(ui.Center).Children(func() {
									if ui.Button(c, "选择 CA 证书").Clicked() {
										path, err := a.chooseCAFile()
										if err != nil {
											view.ConnectionTestText = "选择 CA 证书失败: " + err.Error()
										} else if path != "" {
											view.ConnectionForm.CAFile = path
											view.ConnectionTestText = ""
										}
									}
									label := "未选择，使用系统根证书"
									if view.ConnectionForm.CAFile != "" {
										label = view.ConnectionForm.CAFile
									}
									ui.Text(c, label).FontSize(10).TextColor(theme.TextMuted).SingleLine()
								})
							})
						}
						ui.Field(c, "访问代理", func() {
							ui.Switch(c, &view.ConnectionForm.ProxyEnabled).Label("通过 SSH 跳板机访问")
						})
						if view.ConnectionForm.ProxyEnabled {
							ui.Field(c, "SSH 代理节点", func() {
								ui.TextInput(c, &view.ConnectionForm.ProxyAddress).Placeholder("公网 IP 或域名")
							})
							ui.Field(c, "SSH 端口", func() {
								ui.TextInput(c, &view.ConnectionForm.ProxyPort).Placeholder("22")
							})
							ui.Field(c, "SSH 用户名", func() {
								ui.TextInput(c, &view.ConnectionForm.ProxyUsername).Placeholder("root")
							})
							ui.Field(c, "SSH 密码", func() {
								ui.TextInput(c, &view.ConnectionForm.ProxyPassword).Label("SSH 密码输入框")
							})
							ui.Field(c, "SSH 主机指纹", func() {
								if view.ConnectionHostKeyCandidate != "" {
									ui.Column(c).FillWidth().Gap(7).Padding(10).Radius(10).
										Background(theme.Surface).Border(1, theme.Border).Children(func() {
										ui.Text(c, view.ConnectionHostKeyFingerprint).FontSize(11).Font("monospace").SingleLine()
										ui.Text(c, "请与服务器管理员提供的主机指纹核对。确认后，后续指纹变化将拒绝连接。").
											FontSize(10).TextColor(theme.TextMuted)
										if ui.PrimaryButton(c, "信任并继续").Clicked() {
											view.ConnectionForm.ProxyHostKey = view.ConnectionHostKeyCandidate
											view.ConnectionHostKeyCandidate = ""
											view.ConnectionHostKeyFingerprint = ""
											a.TestConnection(view.ConnectionForm)
										}
									})
								} else if view.ConnectionForm.ProxyHostKey != "" {
									fingerprint, err := kafka.SSHHostKeyFingerprint(view.ConnectionForm.ProxyHostKey)
									if err != nil {
										ui.Text(c, "已保存的主机指纹无效: "+err.Error()).FontSize(11).TextColor(theme.Danger)
									} else {
										ui.Text(c, "已固定 "+fingerprint).FontSize(11).Font("monospace").TextColor(theme.Success).SingleLine()
									}
								} else {
									ui.Text(c, "首次测试连接后会显示 SHA256 指纹，确认前不会建立认证。").
										FontSize(10).TextColor(theme.TextMuted)
								}
							})
						}
					})
				})
			})
			if view.ConnectionTesting {
				ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
					ui.Spinner(c)
					ui.Text(c, "正在测试连接…").FontSize(12).TextColor(theme.TextMuted)
				})
			} else if view.ConnectionTestText != "" {
				color := theme.Danger
				if view.ConnectionTestText == "连接成功" {
					color = theme.Success
				}
				ui.Text(c, view.ConnectionTestText).FontSize(12).TextColor(color)
			}
			ui.Row(c).Gap(8).Justify(ui.End).Children(func() {
				if ui.Button(c, "取消").Clicked() {
					view.ConnectionDialog = false
				}
				if ui.Button(c, "测试连接").Clicked() {
					view.ConnectionTestText = ""
					a.TestConnection(view.ConnectionForm)
					view.ConnectionDialog = true
				}
				if ui.PrimaryButton(c, "保存连接").Clicked() {
					if err := a.SaveConnection(view.ConnectionForm); err != nil {
						view.ConnectionTestText = err.Error()
					} else {
						view.ConnectionDialog = false
						a.RefreshAll()
					}
				}
			})
		})
	})

	deleteTargetID := view.DeleteConnectionID
	deleteTarget, hasDeleteTarget := connectionByID(view.Connections, deleteTargetID)
	deleteTitle := "删除连接？"
	if hasDeleteTarget {
		deleteTitle = fmt.Sprintf("删除连接 %q？", deleteTarget.Name)
	}
	openDelete := view.DeleteDialog
	answer := ui.AlertDialog(c, &openDelete, deleteTitle, "这会移除本地连接配置和系统凭据，不会修改 Kafka 集群。", "取消", "删除")
	view.DeleteDialog = openDelete
	if !view.DeleteDialog {
		view.DeleteConnectionID = ""
	}
	if answer == 1 {
		view.DeleteDialog = false
		view.DeleteConnectionID = ""
		go func() {
			if err := a.DeleteConnection(deleteTargetID); err != nil {
				a.setError(err.Error())
				return
			}
			a.RefreshAll()
		}()
	}

}

func saslMechanismLabel(mechanism string) string {
	switch mechanism {
	case model.SASLPlain:
		return "PLAIN"
	case model.SASLScramSHA256:
		return "SCRAM-SHA-256"
	case model.SASLScramSHA512:
		return "SCRAM-SHA-512"
	default:
		return "不使用"
	}
}

func saslMechanismValue(label string) string {
	switch label {
	case "PLAIN":
		return model.SASLPlain
	case "SCRAM-SHA-256":
		return model.SASLScramSHA256
	case "SCRAM-SHA-512":
		return model.SASLScramSHA512
	default:
		return model.SASLNone
	}
}

func labelValue(label, value string) func(c *ui.Context) {
	return func(c *ui.Context) {
		theme := c.Theme()
		ui.Column(c).Gap(1).Children(func() {
			ui.Text(c, label).FontSize(10).TextColor(theme.TextMuted)
			ui.Text(c, value).FontSize(12).Bold()
		})
	}
}

func compactText(value string, maximum int) string {
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.TrimSpace(value)
	if len([]rune(value)) <= maximum {
		return value
	}
	runes := []rune(value)
	return string(runes[:maximum]) + "…"
}

func humanNumber(value int64) string {
	text := fmt.Sprintf("%d", value)
	for i := len(text) - 3; i > 0; i -= 3 {
		text = text[:i] + "," + text[i:]
	}
	return text
}
