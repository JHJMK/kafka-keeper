package app

import (
	"fmt"
	"strings"

	"github.com/egoist/mygo/ui"

	"github.com/jjk/kafka-keeper/internal/kafka"
	uikit "github.com/jjk/kafka-keeper/internal/ui"
)

func (a *App) groupsPage(c *ui.Context, view *State) {
	theme := c.Theme()
	ui.Row(c).Grow(1).FillWidth().Gap(14).Children(func() {
		ui.Column(c).Width(300).FillHeight().Padding(14).Gap(10).Radius(uikit.PanelRadius).
			Background(theme.Surface).Border(1, theme.Border).Children(func() {
			ui.Row(c).FillWidth().AlignItems(ui.Center).Children(func() {
				ui.Text(c, "Consumer Groups").FontSize(15).Bold()
				ui.Spacer(c).Grow(1)
				ui.Text(c, fmt.Sprintf("%d", len(view.Groups))).FontSize(11).TextColor(theme.TextMuted)
			})
			ui.SearchField(c, &view.GroupFilter).Placeholder("搜索 Group 或状态").FillWidth()
			ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
				ui.Text(c, "仅查看活跃 Group")
				ui.Switch(c.Key("活跃 Group 过滤开关"), &view.ActiveGroupsOnly).Label("活跃 Group 过滤开关")
			})
			inlineError(c, view.GroupsError)
			if view.GroupsLoading {
				loadingRow(c, "正在加载消费组…")
			}
			groups := filterGroups(view.Groups, view.GroupFilter, view.ActiveGroupsOnly)
			if len(groups) == 0 {
				emptyPanel(c, "没有匹配的 Consumer Group", "检查过滤条件或刷新列表。")
				return
			}
			ui.Scroll(c).Grow(1).FillWidth().Gap(6).Children(func() {
				for _, group := range groups {
					group := group
					selected := group.GroupID == view.SelectedGroup
					row := ui.Row(c.Key("group-"+group.GroupID)).FillWidth().Padding(9, 10).Radius(10).Gap(8).
						AlignItems(ui.Center)
					if selected {
						row.Background(theme.Selection).Border(1, theme.Accent)
					} else {
						row.Background(theme.Background).Border(1, theme.Border)
					}
					row.Children(func() {
						ui.Column(c).Grow(1).Gap(1).Children(func() {
							ui.Text(c, group.GroupID).FontSize(12).Bold().SingleLine()
							ui.Text(c, fmt.Sprintf("%s · %d members", group.State, group.MemberCount)).
								FontSize(10).TextColor(theme.TextMuted).SingleLine()
						})
						ui.Column(c).Gap(1).AlignItems(ui.End).Children(func() {
							ui.Text(c, humanNumber(group.TotalLag)).FontSize(11).Bold().TextColor(lagColor(c, group.TotalLag))
							ui.Text(c, "lag").FontSize(9).TextColor(theme.TextMuted)
						})
					})
					if row.Clicked() && !selected {
						view.SelectedGroup = group.GroupID
						view.GroupTab = 0
						view.Consumer.GroupID = group.GroupID
						a.SelectGroup(group.GroupID)
					}
				}
			})
		})

		ui.Column(c).Grow(1).FillHeight().Children(func() {
			if view.SelectedGroup == "" {
				newGroupWorkspace(c, a, view)
				return
			}
			groupWorkspace(c, a, view)
		})
	})
}

func newGroupWorkspace(c *ui.Context, a *App, view *State) {
	theme := c.Theme()
	ui.Column(c).Fill().FillWidth().Padding(16).Gap(12).Radius(uikit.PanelRadius).
		Background(theme.Surface).Border(1, theme.Border).Children(func() {
		ui.Text(c, "新建消费会话").FontSize(19).Bold()
		ui.Text(c, "输入新的 Consumer Group 名称和 Topic，启动后会真实加入 Group。").
			FontSize(10).TextColor(theme.TextMuted)
		ui.Divider(c)
		groupConsumer(c, a, view)
	})
}

func groupWorkspace(c *ui.Context, a *App, view *State) {
	theme := c.Theme()
	ui.Column(c).Fill().FillWidth().Padding(16).Gap(12).Radius(uikit.PanelRadius).
		Background(theme.Surface).Border(1, theme.Border).Children(func() {
		ui.Row(c).FillWidth().AlignItems(ui.Center).Children(func() {
			ui.Column(c).Gap(1).Children(func() {
				ui.Text(c, view.SelectedGroup).FontSize(19).Bold().SingleLine()
				ui.Text(c, fmt.Sprintf("%s · %d members · lag %s", fallback(view.GroupDetail.Summary.State, "UNKNOWN"), len(view.GroupDetail.Members), humanNumber(view.GroupDetail.Summary.TotalLag))).
					FontSize(10).TextColor(theme.TextMuted)
			})
			ui.Spacer(c).Grow(1)
			if view.GroupDetailLoading {
				ui.Spinner(c)
			}
			if ui.Button(c, "刷新详情").Clicked() {
				a.RefreshGroupDetail()
			}
		})
		inlineError(c, view.GroupDetailError)
		ui.Tabs(c, &view.GroupTab, "Group 详情", "消费消息")
		ui.Divider(c)
		if view.GroupTab == 1 {
			groupConsumer(c, a, view)
			return
		}
		groupDetail(c, view)
	})
}

func groupDetail(c *ui.Context, view *State) {
	theme := c.Theme()
	if view.GroupDetailLoading && view.GroupDetail.Summary.GroupID == "" {
		loadingRow(c, "正在读取 Group 详情…")
		return
	}
	ui.Scroll(c).Grow(1).FillWidth().Gap(12).Children(func() {
		ui.Row(c).FillWidth().Gap(10).Children(func() {
			metricCard(c, "State", fallback(view.GroupDetail.Summary.State, "—"), "消费组状态")
			metricCard(c, "Members", fmt.Sprintf("%d", len(view.GroupDetail.Members)), "活跃成员")
			metricCard(c, "Topics", fmt.Sprintf("%d", len(view.GroupDetail.Topics)), "已分配 Topic")
			metricCard(c, "Total Lag", humanNumber(view.GroupDetail.Summary.TotalLag), "累计积压")
		})
		ui.Row(c).FillWidth().Gap(16).Children(func() {
			labelValue("Protocol Type", fallback(view.GroupDetail.ProtocolType, "—"))(c)
			labelValue("Protocol", fallback(view.GroupDetail.Protocol, "—"))(c)
			labelValue("Coordinator", fallback(view.GroupDetail.Summary.Coordinator, "—"))(c)
		})
		ui.Text(c, "成员分配").FontSize(13).Bold()
		if len(view.GroupDetail.Members) == 0 {
			ui.Text(c, "当前没有活跃成员。").FontSize(11).TextColor(theme.TextMuted)
		} else {
			columns := []ui.TableColumn{
				{Title: "Member ID", MinWidth: 190},
				{Title: "Client ID", MinWidth: 130},
				{Title: "Host", Width: 140},
				{Title: "Assignments", MinWidth: 220},
			}
			ui.Table(c, nil, columns, len(view.GroupDetail.Members), func(row, col int) {
				member := view.GroupDetail.Members[row]
				switch col {
				case 0:
					ui.Text(c, member.MemberID).Font("monospace").FontSize(10).SingleLine()
				case 1:
					ui.Text(c, fallback(member.ClientID, "—")).SingleLine()
				case 2:
					ui.Text(c, fallback(member.Host, "—")).Font("monospace").FontSize(10).SingleLine()
				case 3:
					ui.Text(c, assignmentsText(member.Assignments)).Font("monospace").FontSize(10).SingleLine()
				}
			}).Height(180).FillWidth()
		}
		ui.Text(c, "Offset 与 Lag").FontSize(13).Bold()
		if len(view.GroupDetail.Offsets) == 0 {
			ui.Text(c, "没有已提交的 Offset。").FontSize(11).TextColor(theme.TextMuted)
			return
		}
		columns := []ui.TableColumn{
			{Title: "Topic", MinWidth: 180},
			{Title: "Partition", Width: 100},
			{Title: "Current", Width: 130},
			{Title: "End", Width: 130},
			{Title: "Lag", Width: 110},
		}
		ui.Table(c, nil, columns, len(view.GroupDetail.Offsets), func(row, col int) {
			offset := view.GroupDetail.Offsets[row]
			switch col {
			case 0:
				ui.Text(c, offset.Topic).SingleLine()
			case 1:
				ui.Text(c, fmt.Sprintf("%d", offset.Partition)).Font("monospace")
			case 2:
				ui.Text(c, humanNumber(offset.Current)).Font("monospace")
			case 3:
				ui.Text(c, humanNumber(offset.End)).Font("monospace")
			case 4:
				ui.Text(c, humanNumber(offset.Lag)).Font("monospace").TextColor(lagColor(c, offset.Lag))
			}
		}).Height(240).FillWidth()
	})
}

func groupConsumer(c *ui.Context, a *App, view *State) {
	theme := c.Theme()
	ui.Column(c).Grow(1).FillWidth().Gap(9).Children(func() {
		ui.Row(c).FillWidth().Gap(12).Children(func() {
			ui.Column(c).Grow(1).Gap(2).Children(func() {
				ui.Text(c, "显式加入 Consumer Group").FontSize(13).Bold()
				ui.Text(c, "此页面会真实加入 Group 并持续 Poll。同一 Kafka 连接同时只允许一个活动消费会话。").
					FontSize(10).TextColor(theme.TextMuted).Wrap()
			})
			if view.ConsumerSession != nil {
				badge(c, "消费中", theme.Success)
			}
		})
		ui.Row(c).FillWidth().Gap(8).AlignItems(ui.End).Children(func() {
			ui.Field(c, "Consumer Group", func() {
				ui.TextInput(c, &view.Consumer.GroupID).FillWidth().Disabled(view.ConsumerSession != nil)
			})
			ui.Field(c, "Topic", func() {
				ui.TextInput(c, &view.Consumer.Topic).FillWidth().Placeholder("输入 Topic 名称").Disabled(view.ConsumerSession != nil)
			})
			ui.Column(c).Width(190).Gap(5).Children(func() {
				ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
					ui.Text(c, "从最早 Offset 开始").FontSize(10).TextColor(theme.TextMuted)
					ui.Switch(c.Key("从最早开始开关"), &view.Consumer.FromStart).
						Label("从最早 Offset 开始").Disabled(view.ConsumerSession != nil)
				})
				ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
					ui.Text(c, "自动提交 Offset").FontSize(10).TextColor(theme.TextMuted)
					ui.Switch(c.Key("自动提交开关"), &view.Consumer.AutoCommit).
						Label("自动提交 Offset").Disabled(view.ConsumerSession != nil)
				})
			})
		})
		if len(view.GroupDetail.Topics) > 0 && view.ConsumerSession == nil {
			ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
				ui.Text(c, "使用 Group Topic:").FontSize(10).TextColor(theme.TextMuted)
				for _, topic := range view.GroupDetail.Topics {
					topic := topic
					if ui.Button(c, topic).Clicked() {
						view.Consumer.Topic = topic
					}
				}
			})
		}
		if view.Consumer.AutoCommit && view.ConsumerSession == nil {
			ui.Text(c, "自动提交可能在你处理完消息前推进 Offset；需要精确控制时请关闭它并手动提交。").
				FontSize(10).TextColor(theme.Danger).Wrap()
		}
		if view.Consumer.FromStart && view.ConsumerSession == nil {
			ui.Text(c, "From Start/Latest 仅在 Group 没有可用已提交 Offset 时作为 Reset 策略生效。").
				FontSize(10).TextColor(theme.TextMuted).Wrap()
		}
		ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
			if view.ConsumerSession == nil {
				if ui.PrimaryButton(c, "开始消费").Disabled(view.ConsumerBusy).Clicked() {
					a.StartConsumerSession()
				}
			} else {
				if view.ConsumerPaused {
					if ui.Button(c, "继续 Poll").Clicked() {
						a.ResumeConsumer()
					}
				} else if ui.Button(c, "暂停").Clicked() {
					a.PauseConsumer()
				}
				if !view.Consumer.AutoCommit {
					if ui.Button(c, "提交 Offset").Clicked() {
						a.CommitConsumer()
					}
				}
				if ui.Button(c, "停止消费").Clicked() {
					a.StopConsumer()
				}
			}
			if view.ConsumerBusy {
				ui.Spinner(c)
				ui.Text(c, "正在加入 Group…").FontSize(10).TextColor(theme.TextMuted)
			}
		})
		inlineError(c, view.ConsumerError)
		ui.Row(c).FillWidth().AlignItems(ui.Center).Children(func() {
			ui.Text(c, "本次会话已收到").FontSize(11).TextColor(theme.TextMuted)
			ui.Text(c, fmt.Sprintf(" %d ", len(view.ConsumerRecords))).FontSize(11).Bold().TextColor(theme.Accent)
			ui.Text(c, "条消息（界面最多保留 1000 条）").FontSize(11).TextColor(theme.TextMuted)
		})
		if len(view.ConsumerRecords) == 0 {
			emptyPanel(c, "等待消息", "启动消费后，记录会实时追加到这里。")
			return
		}
		ui.Scroll(c).Grow(1).FillWidth().Gap(6).Children(func() {
			for i, record := range view.ConsumerRecords {
				message := record.Message
				row := ui.Row(c.Key(fmt.Sprintf("consumer-%d-%d-%d", i, message.Partition, message.Offset))).
					FillWidth().Padding(8, 10).Radius(9).Gap(10).AlignItems(ui.Center).
					Background(theme.Background).Border(1, theme.Border)
				row.Children(func() {
					ui.Column(c).Width(100).Gap(1).Children(func() {
						ui.Text(c, fmt.Sprintf("P%d / O%d", message.Partition, message.Offset)).FontSize(10).Bold().TextColor(theme.Accent).SingleLine()
						ui.Text(c, formatTime(message.Timestamp)).FontSize(9).TextColor(theme.TextMuted).SingleLine()
					})
					ui.Column(c).Grow(1).Gap(1).Children(func() {
						ui.Text(c, "Key: "+compactText(message.KeyText(), 40)).FontSize(10).TextColor(theme.TextMuted).SingleLine()
						ui.Text(c, messageSummary(message)).FontSize(11).SingleLine()
					})
					ui.Text(c, fmt.Sprintf("%d headers", len(message.Headers))).FontSize(9).TextColor(theme.TextMuted)
				})
			}
		})
	})
}

func assignmentsText(assignments []kafka.Assignment) string {
	if len(assignments) == 0 {
		return "—"
	}
	parts := make([]string, 0, len(assignments))
	for _, assignment := range assignments {
		parts = append(parts, fmt.Sprintf("%s/%d", assignment.Topic, assignment.Partition))
	}
	return strings.Join(parts, ", ")
}

func lagColor(c *ui.Context, lag int64) ui.Color {
	if lag > 0 {
		return c.Theme().Danger
	}
	return c.Theme().Success
}
