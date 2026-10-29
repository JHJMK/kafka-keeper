package app

import (
	"fmt"
	"strings"

	"github.com/egoist/mygo/ui"

	"github.com/jjk/kafka-keeper/internal/kafka"
	uikit "github.com/jjk/kafka-keeper/internal/ui"
)

func (a *App) topicsPage(c *ui.Context, view *State) {
	theme := c.Theme()
	ui.Row(c).Grow(1).FillWidth().Gap(14).Children(func() {
		ui.Column(c).Width(300).FillHeight().Padding(14).Gap(10).Radius(uikit.PanelRadius).
			Background(theme.Surface).Border(1, theme.Border).Children(func() {
			ui.Row(c).FillWidth().AlignItems(ui.Center).Children(func() {
				ui.Text(c, "Topics").FontSize(15).Bold()
				ui.Spacer(c).Grow(1)
				ui.Text(c, fmt.Sprintf("%d", len(view.Topics))).FontSize(11).TextColor(theme.TextMuted)
			})
			ui.SearchField(c, &view.TopicFilter).Placeholder("搜索 Topic").FillWidth()
			inlineError(c, view.TopicsError)
			if view.TopicsLoading {
				loadingRow(c, "正在加载 Topics…")
			}
			topics := filterTopics(view.Topics, view.TopicFilter)
			if len(topics) == 0 {
				emptyPanel(c, "没有匹配的 Topic", "检查过滤条件或刷新 Topic 列表。")
				return
			}
			ui.Scroll(c).Grow(1).FillWidth().Gap(6).Children(func() {
				for _, topic := range topics {
					topic := topic
					selected := topic.Name == view.SelectedTopic
					row := ui.Row(c.Key("topic-"+topic.Name)).FillWidth().Padding(9, 10).Radius(10).Gap(8).
						AlignItems(ui.Center)
					if selected {
						row.Background(theme.Selection).Border(1, theme.Accent)
					} else {
						row.Background(theme.Background).Border(1, theme.Border)
					}
					row.Children(func() {
						ui.Column(c).Grow(1).Gap(1).Children(func() {
							ui.Text(c, topic.Name).FontSize(12).Bold().SingleLine()
							ui.Text(c, fmt.Sprintf("%d partitions", topic.PartitionCount)).FontSize(10).TextColor(theme.TextMuted)
						})
						if topic.Internal {
							badge(c, "internal", theme.TextMuted)
						}
					})
					if row.Clicked() && !selected {
						view.SelectedTopic = topic.Name
						view.TopicTab = 0
						a.SelectTopic(topic.Name)
					}
				}
			})
		})

		ui.Column(c).Grow(1).FillHeight().Children(func() {
			if view.SelectedTopic == "" {
				panel(c, 0, func() {
					emptyPanel(c, "选择一个 Topic", "查看分区、配置，并浏览或搜索 Key / Header 元数据。")
				})
				return
			}
			topicDetail(c, a, view)
		})
	})
}

func topicDetail(c *ui.Context, a *App, view *State) {
	theme := c.Theme()
	ui.Column(c).Fill().FillWidth().Padding(16).Gap(12).Radius(uikit.PanelRadius).
		Background(theme.Surface).Border(1, theme.Border).Children(func() {
		ui.Row(c).FillWidth().AlignItems(ui.Center).Children(func() {
			ui.Column(c).Gap(1).Children(func() {
				ui.Text(c, view.SelectedTopic).FontSize(19).Bold().SingleLine()
				ui.Text(c, fmt.Sprintf("%d partitions · %d groups", view.TopicDetail.Summary.PartitionCount, len(view.TopicDetail.Groups))).
					FontSize(10).TextColor(theme.TextMuted)
			})
			ui.Spacer(c).Grow(1)
			if view.TopicDetailLoading {
				ui.Spinner(c)
			}
			if ui.Button(c, "刷新详情").Clicked() {
				a.RefreshTopicDetail()
			}
		})
		inlineError(c, view.TopicDetailError)
		ui.Tabs(c, &view.TopicTab, "分区", "配置", "浏览消息", "查找消息")
		ui.Divider(c)
		switch view.TopicTab {
		case 0:
			topicPartitions(c, view)
		case 1:
			topicConfigs(c, view)
		case 2:
			topicBrowser(c, a, view)
		case 3:
			topicSearch(c, a, view)
		}
	})
}

func topicPartitions(c *ui.Context, view *State) {
	if view.TopicDetailLoading && len(view.TopicDetail.Partitions) == 0 {
		loadingRow(c, "正在读取分区…")
		return
	}
	if len(view.TopicDetail.Partitions) == 0 {
		emptyPanel(c, "没有分区数据", "刷新 Topic 详情后重试。")
		return
	}
	columns := []ui.TableColumn{
		{Title: "Partition", MinWidth: 64},
		{Title: "Leader", MinWidth: 64},
		{Title: "Replicas", MinWidth: 120},
		{Title: "Start", MinWidth: 76},
		{Title: "End", MinWidth: 76},
		{Title: "Messages", MinWidth: 76},
	}
	ui.Table(c, nil, columns, len(view.TopicDetail.Partitions), func(row, col int) {
		partition := view.TopicDetail.Partitions[row]
		switch col {
		case 0:
			ui.Text(c, fmt.Sprintf("%d", partition.ID)).Font("monospace")
		case 1:
			ui.Text(c, fmt.Sprintf("%d", partition.Leader)).Font("monospace")
		case 2:
			ui.Text(c, formatInt32s(partition.Replicas)).Font("monospace").SingleLine()
		case 3:
			ui.Text(c, humanNumber(partition.StartOffset)).Font("monospace")
		case 4:
			ui.Text(c, humanNumber(partition.EndOffset)).Font("monospace")
		case 5:
			ui.Text(c, humanNumber(max(partition.EndOffset-partition.StartOffset, 0))).Font("monospace")
		}
	}).Grow(1).FillWidth()
}

func topicConfigs(c *ui.Context, view *State) {
	if view.TopicDetail.ConfigError != "" {
		inlineError(c, view.TopicDetail.ConfigError)
	}
	if len(view.TopicDetail.Configs) == 0 {
		if view.TopicDetail.ConfigError != "" {
			return
		}
		emptyPanel(c, "没有配置数据", "该 Topic 可能未返回显式配置。")
		return
	}
	columns := []ui.TableColumn{
		{Title: "配置项", MinWidth: 220},
		{Title: "值", MinWidth: 220},
		{Title: "来源", Width: 120},
	}
	ui.Table(c, nil, columns, len(view.TopicDetail.Configs), func(row, col int) {
		config := view.TopicDetail.Configs[row]
		switch col {
		case 0:
			ui.Text(c, config.Name).Font("monospace").SingleLine()
		case 1:
			value := config.Value
			if config.Sensitive {
				value = "••••••••"
			}
			ui.Text(c, value).Font("monospace").SingleLine()
		case 2:
			ui.Text(c, fallback(config.Source, "—")).FontSize(11).TextColor(c.Theme().TextMuted)
		}
	}).Grow(1).FillWidth()
}

func topicBrowser(c *ui.Context, a *App, view *State) {
	theme := c.Theme()
	ui.Column(c).Grow(1).FillWidth().Gap(10).Children(func() {
		ui.Text(c, "浏览消息").FontSize(13).Bold()
		ui.Text(c, "不加入 Consumer Group，直接按 Offset 或时间读取。").
			FontSize(10).TextColor(theme.TextMuted)

		ui.Column(c).Width(360).Gap(4).Children(func() {
			ui.Text(c, "读取方式").FontSize(10).TextColor(theme.TextMuted)
			modes := []string{"按 Offset", "按时间", "时间范围"}
			index := browseModeIndex(view.Browse.Mode)
			ui.Tabs(c, &index, modes...)
			view.Browse.Mode = browseModeValue(index)
		})

		switch view.Browse.Mode {
		case "time":
			ui.Field(c, "起始时间", func() {
				dateTimeInput(c, &view.Browse.Start, "起始日期", "起始时刻", false)
			})
		case "range":
			ui.Row(c).Gap(20).AlignItems(ui.End).Children(func() {
				ui.Field(c, "开始时间", func() {
					dateTimeInput(c, &view.Browse.Start, "开始日期", "开始时刻", false)
				})
				ui.Field(c, "结束时间", func() {
					dateTimeInput(c, &view.Browse.End, "结束日期", "结束时刻", false)
				})
			})
		default:
			ui.Field(c, "Offset", func() {
				ui.TextInput(c, &view.Browse.Offset).Placeholder("0").Width(180)
			})
		}

		ui.Row(c).FillWidth().Gap(14).AlignItems(ui.Center).Children(func() {
			ui.Text(c, "条数上限").FontSize(10).TextColor(theme.TextMuted)
			ui.TextInput(c, &view.Browse.Limit).Placeholder("100").Width(100)
			ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
				ui.Text(c, "全部分区").FontSize(10).TextColor(theme.TextMuted)
				ui.Switch(c.Key("全部分区开关"), &view.Browse.AllPartitions).Label("全部分区开关")
			})
		})

		if !view.Browse.AllPartitions {
			ui.Field(c, "Partition", func() {
				ui.TextInput(c, &view.Browse.Partition).Placeholder("0").Width(140)
			})
		}

		ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
			if ui.PrimaryButton(c, "浏览消息").Disabled(view.BrowseLoading).Clicked() {
				a.BrowseMessages()
			}
			if view.BrowseLoading {
				ui.Spinner(c)
			}
		})
		inlineError(c, view.BrowseError)
		messageResults(c, view, view.BrowseResults, "browse", view.BrowseLoaded)
	})
}

func topicSearch(c *ui.Context, a *App, view *State) {
	theme := c.Theme()
	ui.Column(c).Grow(1).FillWidth().Gap(9).Children(func() {
		ui.Text(c, "查找消息").FontSize(13).Bold()
		ui.Text(c, "仅搜索 Key 与 Header，不搜索消息 Value。按 Offset 从小到大扫描指定范围。").
			FontSize(10).TextColor(theme.TextMuted).Wrap()
		ui.Row(c).FillWidth().Gap(8).AlignItems(ui.Start).Children(func() {
			ui.Field(c, "Key", func() {
				ui.TextInput(c, &view.Search.Key).Placeholder(keySearchPlaceholder(view.Search.KeyContains)).Width(210)
			})
			ui.Field(c, "Key 子串匹配", func() {
				ui.Switch(c, &view.Search.KeyContains)
			})
		})
		ui.Row(c).FillWidth().Gap(8).AlignItems(ui.End).Children(func() {
			ui.Field(c, "Header Key", func() {
				ui.TextInput(c, &view.Search.HeaderKey).Placeholder("trace-id").Width(180)
			})
			ui.Field(c, "Header Value", func() {
				ui.TextInput(c, &view.Search.HeaderValue).Placeholder("可选").Width(180)
			})
		})
		ui.Row(c).FillWidth().AlignItems(ui.End).Children(func() {
			ui.Field(c, "开始时间", func() {
				ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
					ui.Row(c).Gap(5).AlignItems(ui.Center).Children(func() {
						ui.Text(c, "启用").FontSize(10).TextColor(theme.TextMuted)
						ui.Switch(c.Key("开始时间开关"), &view.Search.StartEnabled).Label("启用开始时间")
					})
					dateTimeInput(c, &view.Search.Start, "开始日期", "开始时刻", !view.Search.StartEnabled)
				})
			})
		})
		ui.Row(c).FillWidth().AlignItems(ui.End).Children(func() {
			ui.Field(c, "结束时间", func() {
				ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
					ui.Row(c).Gap(5).AlignItems(ui.Center).Children(func() {
						ui.Text(c, "启用").FontSize(10).TextColor(theme.TextMuted)
						ui.Switch(c.Key("结束时间开关"), &view.Search.EndEnabled).Label("启用结束时间")
					})
					dateTimeInput(c, &view.Search.End, "结束日期", "结束时刻", !view.Search.EndEnabled)
				})
			})
		})
		ui.Row(c).FillWidth().Gap(8).AlignItems(ui.End).Children(func() {
			ui.Field(c, "Partition (-1 全部)", func() {
				ui.TextInput(c, &view.Search.Partition).Placeholder("-1").Width(110)
			})
			ui.Field(c, "起始 Offset", func() {
				ui.TextInput(c, &view.Search.OffsetStart).Placeholder("可选").Width(105)
			})
			ui.Field(c, "结束 Offset", func() {
				ui.TextInput(c, &view.Search.OffsetEnd).Placeholder("可选").Width(105)
			})
		})
		ui.Row(c).FillWidth().Gap(8).AlignItems(ui.End).Children(func() {
			ui.Field(c, "最大扫描数", func() {
				ui.TextInput(c, &view.Search.MaxScan).Placeholder("5000").Width(100)
			})
			ui.Field(c, "最大结果数", func() {
				ui.TextInput(c, &view.Search.MaxResults).Placeholder("100").Width(100)
			})
		})

		ui.Row(c).FillWidth().Gap(8).AlignItems(ui.Center).Children(func() {
			if ui.PrimaryButton(c, "开始查找").Disabled(view.SearchLoading).Clicked() {
				a.SearchMessages()
			}
			if view.SearchLoading {
				ui.Spinner(c)
			}
		})
		inlineError(c, view.SearchError)
		if view.SearchLoaded {
			ui.Text(c, fmt.Sprintf("已扫描 %s 条记录，返回 %d 条结果%s", humanNumber(int64(view.SearchScanned)), len(view.SearchResults), truncatedText(view.SearchTruncated))).
				FontSize(10).TextColor(theme.TextMuted)
		}
		messageResults(c, view, view.SearchResults, "search", view.SearchLoaded)
	})
}

func messageResults(c *ui.Context, view *State, messages []kafka.Message, keyPrefix string, loaded bool) {
	theme := c.Theme()
	if loaded && len(messages) == 0 {
		emptyPanel(c, "没有消息", "当前条件下没有读取到匹配消息。")
		return
	}
	if len(messages) == 0 {
		return
	}
	ui.Row(c).Grow(1).FillWidth().Gap(10).Children(func() {
		ui.Scroll(c).Grow(1).FillHeight().Gap(6).Children(func() {
			for i, message := range messages {
				i, message := i, message
				selected := i == view.SelectedMessage
				row := ui.Row(c.Key(fmt.Sprintf("%s-%d", keyPrefix, i))).FillWidth().Padding(8, 10).Radius(9).
					Gap(8).AlignItems(ui.Center)
				if selected {
					row.Background(theme.Selection).Border(1, theme.Accent)
				} else {
					row.Background(theme.Background).Border(1, theme.Border)
				}
				row.Children(func() {
					ui.Column(c).Width(82).Gap(1).Children(func() {
						ui.Text(c, fmt.Sprintf("P%d", message.Partition)).FontSize(10).Bold().TextColor(theme.Accent)
						ui.Text(c, humanNumber(message.Offset)).FontSize(9).TextColor(theme.TextMuted).SingleLine()
					})
					ui.Column(c).Grow(1).Gap(1).Children(func() {
						ui.Text(c, messageSummary(message)).FontSize(11).SingleLine()
						ui.Text(c, formatTime(message.Timestamp)).FontSize(9).TextColor(theme.TextMuted)
					})
				})
				if row.Clicked() {
					view.SelectedMessage = i
				}
			}
		})
		ui.Column(c).Width(340).FillHeight().Padding(12).Gap(8).Radius(uikit.PanelRadius).
			Background(theme.Background).Border(1, theme.Border).Children(func() {
			if view.SelectedMessage >= 0 && view.SelectedMessage < len(messages) {
				messageDetail(c, messages[view.SelectedMessage])
			} else {
				emptyPanel(c, "选择消息", "点击左侧消息查看 Key、Value 与 Headers。")
			}
		})
	})
}

func browseModeIndex(mode string) int {
	switch mode {
	case "time":
		return 1
	case "range":
		return 2
	default:
		return 0
	}
}

func browseModeValue(index int) string {
	switch index {
	case 1:
		return "time"
	case 2:
		return "range"
	default:
		return "offset"
	}
}

func formatInt32s(values []int32) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d", value))
	}
	if len(parts) == 0 {
		return "—"
	}
	return strings.Join(parts, ", ")
}

func truncatedText(truncated bool) string {
	if truncated {
		return "（已达到扫描或结果上限）"
	}
	return ""
}
