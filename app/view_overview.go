package app

import (
	"fmt"

	"github.com/egoist/mygo/ui"
)

func (a *App) overviewPage(c *ui.Context, view *State) {
	theme := c.Theme()
	ui.Column(c).Grow(1).FillWidth().Gap(12).Children(func() {
		if view.OverviewLoading && !view.OverviewReady {
			panel(c, 0, func() { loadingRow(c, "正在读取集群概览…") })
		}
		inlineError(c, view.OverviewError)
		if !view.OverviewReady {
			if !view.OverviewLoading {
				emptyPanel(c, "等待集群数据", "点击右上角刷新，读取 Broker、Topic 和 Consumer Group 信息。")
			}
			return
		}

		ui.Row(c).FillWidth().Gap(12).Children(func() {
			metricCard(c, "Brokers", fmt.Sprintf("%d", len(view.Overview.Brokers)), "在线 Broker 节点")
			metricCard(c, "Topics", humanNumber(int64(view.Overview.TopicCount)), "可见 Topic 数量")
			metricCard(c, "Consumer Groups", humanNumber(int64(view.Overview.GroupCount)), "已登记消费组")
			metricCard(c, "Total Lag", humanNumber(view.Overview.TotalLag), "所有消费组累计积压")
		})

		panel(c, 0, func() {
			ui.Row(c).FillWidth().AlignItems(ui.Center).Children(func() {
				ui.Column(c).Gap(1).Children(func() {
					ui.Text(c, "Broker 节点").FontSize(15).Bold()
					ui.Text(c, "Cluster ID: "+fallback(view.Overview.ClusterID, "未知")).FontSize(10).TextColor(theme.TextMuted)
				})
				ui.Spacer(c).Grow(1)
				if view.OverviewLoading {
					ui.Spinner(c)
				}
			})
			if len(view.Overview.Brokers) == 0 {
				emptyPanel(c, "没有 Broker 信息", "连接已建立，但元数据中未返回 Broker。")
				return
			}
			columns := []ui.TableColumn{
				{Title: "Node ID", Width: 100},
				{Title: "Host", MinWidth: 160},
				{Title: "Port", Width: 100},
				{Title: "角色", Width: 120},
			}
			ui.Table(c, nil, columns, len(view.Overview.Brokers), func(row, col int) {
				broker := view.Overview.Brokers[row]
				switch col {
				case 0:
					ui.Text(c, fmt.Sprintf("%d", broker.ID)).Font("monospace")
				case 1:
					ui.Text(c, broker.Host).SingleLine()
				case 2:
					ui.Text(c, fmt.Sprintf("%d", broker.Port)).Font("monospace")
				case 3:
					if broker.Controller {
						badge(c, "Controller", theme.Accent)
					} else {
						ui.Text(c, "Broker").FontSize(11).TextColor(theme.TextMuted)
					}
				}
			}).Grow(1).FillWidth()
		})
	})
}

func fallback(value, other string) string {
	if value == "" {
		return other
	}
	return value
}
