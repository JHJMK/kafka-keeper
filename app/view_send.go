package app

import (
	"fmt"
	"github.com/egoist/mygo/ui"

	uikit "github.com/jjk/kafka-keeper/internal/ui"
)

func (a *App) sendPage(c *ui.Context, view *State) {
	theme := c.Theme()
	ui.Scroll(c).Grow(1).FillWidth().Gap(12).Children(func() {
		ui.Row(c).FillWidth().AlignItems(ui.Start).Gap(14).Children(func() {
			ui.Column(c).Grow(1).FillWidth().Padding(18).Gap(12).Radius(uikit.PanelRadius).
				Background(theme.Surface).Border(1, theme.Border).Children(func() {
				ui.Column(c).Gap(2).Children(func() {
					ui.Text(c, "消息内容").FontSize(15).Bold()
					ui.Text(c, "Key、Value、Headers 与时间戳会原样交给 franz-go。").
						FontSize(10).TextColor(theme.TextMuted)
				})
				ui.Form(c, func() {
					ui.Field(c, "Topic", func() {
						topicCombobox(c, view)
					})
					ui.Row(c).FillWidth().Gap(10).AlignItems(ui.End).Children(func() {
						ui.Column(c).Width(180).Gap(5).Children(func() {
							ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
								ui.Text(c, "自动分区")
								ui.Switch(c.Key("自动分区开关"), &view.Produce.AutoPartition).Label("自动分区开关")
							})
						})
						if !view.Produce.AutoPartition {
							ui.Field(c, "Partition", func() {
								ui.TextInput(c, &view.Produce.Partition).Placeholder("0").Width(120)
							})
						}
					})
					ui.Field(c, "Key", func() {
						ui.TextInput(c, &view.Produce.Key).Placeholder("可选；UTF-8 字符串")
					})
					ui.Field(c, "Value", func() {
						ui.TextArea(c, &view.Produce.Value).Placeholder("消息正文，可输入多行文本").Lines(5, 12).FillWidth().Font("monospace")
					})
					ui.Field(c, "Headers", func() {
						ui.TextArea(c, &view.Produce.Headers).Placeholder("每行一个：trace-id: abc123").Lines(3, 8).FillWidth().Font("monospace")
					})
					ui.Column(c).Gap(3).Children(func() {
						ui.Text(c, "压缩方式").FontSize(10).TextColor(theme.TextMuted)
						ui.Select(c, &view.Produce.Compression, []string{"none", "gzip", "snappy", "lz4", "zstd"}).FillWidth()
					})
					ui.Field(c, "消息时间（可选）", func() {
						ui.Row(c).Gap(10).AlignItems(ui.Center).Children(func() {
							ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
								ui.Text(c, "指定消息时间").FontSize(10).TextColor(theme.TextMuted)
								ui.Switch(c.Key("消息时间开关"), &view.Produce.TimestampEnabled).Label("指定消息时间")
							})
							dateTimeInput(c, &view.Produce.Timestamp, "消息日期", "消息时刻", !view.Produce.TimestampEnabled)
						})
					})
				})
				inlineError(c, view.ProduceError)
				ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
					if ui.PrimaryButton(c, "发送消息").Disabled(view.ProduceBusy).Clicked() {
						a.ProduceMessage()
					}
					if view.ProduceBusy {
						ui.Spinner(c)
						ui.Text(c, "正在发送…").FontSize(11).TextColor(theme.TextMuted)
					}
				})
			})

			ui.Column(c).Width(340).FillWidth().Gap(12).Children(func() {
				ui.Column(c).FillWidth().Padding(16).Gap(10).Radius(uikit.PanelRadius).
					Background(theme.Surface).Border(1, theme.Border).Children(func() {
					ui.Text(c, "发送结果").FontSize(15).Bold()
					if view.LastProduce == nil {
						ui.Text(c, "发送成功后，这里会显示目标 Topic、Partition、Offset 和耗时。").
							FontSize(11).TextColor(theme.TextMuted).Wrap()
						return
					}
					result := view.LastProduce
					badge(c, "发送成功", theme.Success)
					labelValue("Topic", result.Topic)(c)
					labelValue("Partition", fmt.Sprintf("%d", result.Partition))(c)
					labelValue("Offset", humanNumber(result.Offset))(c)
					labelValue("耗时", result.Elapsed.Round(1000*1000).String())(c)
				})
				ui.Column(c).FillWidth().Padding(16).Gap(8).Radius(uikit.PanelRadius).
					Background(theme.Background).Border(1, theme.Border).Children(func() {
					ui.Text(c, "发送约定").FontSize(12).Bold()
					for _, line := range []string{
						"自动分区使用 sticky partitioner。",
						"关闭自动分区后使用 manual partitioner。",
						"Header 每行使用 key: value 格式。",
						"未指定消息时间时，由 franz-go 使用当前时间。",
					} {
						ui.Text(c, "· "+line).FontSize(10).TextColor(theme.TextMuted).Wrap()
					}
				})
			})
		})
	})
}

func topicCombobox(c *ui.Context, view *State) {
	theme := c.Theme()
	cb := ui.ComboboxBase(c.Key("Topic 下拉输入框"), &view.Produce.Topic)
	cb.Input.Label("Topic 下拉输入框").Placeholder("例如：orders.created").FillWidth().
		Padding(6, 10).Radius(8).Background(theme.Background).Border(1, theme.Border)

	options := topicNames(view.Topics)
	if cb.Filtering() {
		options = filterTopicOptions(options, view.Produce.Topic)
	}
	cb.Popup(func(panel ui.Element) {
		panel.Padding(5).Gap(2).Radius(9).Background(theme.Surface).Border(1, theme.Border)
		for _, topic := range options {
			item := cb.Item(topic).Padding(6, 9).Radius(7)
			if item.Highlighted() {
				item.Background(theme.Selection).TextColor(theme.Accent)
			}
			item.Children(func() {
				ui.Text(c, topic).FontSize(11).SingleLine()
			})
		}
	})
	if topic, ok := cb.Chosen(); ok {
		view.Produce.Topic = topic
	}
}
