package app

import (
	"fmt"
	"strings"
	"time"

	"github.com/egoist/mygo/ui"

	"github.com/jjk/kafka-keeper/internal/kafka"
	uikit "github.com/jjk/kafka-keeper/internal/ui"
)

func panel(c *ui.Context, width float32, fn func()) ui.Element {
	theme := c.Theme()
	e := ui.Column(c).FillWidth().Padding(16).Gap(12).Radius(uikit.PanelRadius).
		Background(theme.Surface).Border(1, theme.Border)
	if width > 0 {
		e.Width(width)
	}
	e.Children(fn)
	return e
}

func emptyPanel(c *ui.Context, title, detail string) {
	theme := c.Theme()
	ui.Column(c).Fill().FillWidth().Center().Children(func() {
		ui.Column(c).Width(360).Gap(6).Center().Children(func() {
			ui.Text(c, title).FontSize(15).Bold()
			ui.Text(c, detail).FontSize(11).TextColor(theme.TextMuted).TextAlign(ui.Center).Wrap()
		})
	})
}

func loadingRow(c *ui.Context, label string) {
	theme := c.Theme()
	ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
		ui.Spinner(c)
		ui.Text(c, label).FontSize(12).TextColor(theme.TextMuted)
	})
}

func dateTimeInput(c *ui.Context, value *time.Time, dateLabel, timeLabel string, disabled bool) {
	if value.IsZero() {
		*value = c.Now().Truncate(time.Minute)
	}
	e := ui.Row(c).Gap(8).AlignItems(ui.Center).Disabled(disabled)
	e.Children(func() {
		ui.DateInput(c, value).Label(dateLabel).Width(128)
		ui.TimeInput(c, value).Label(timeLabel).Width(84)
	})
}

func inlineError(c *ui.Context, message string) {
	if message == "" {
		return
	}
	ui.Text(c, message).FontSize(11).TextColor(c.Theme().Danger).Wrap()
}

func metricCard(c *ui.Context, label, value, detail string) {
	theme := c.Theme()
	ui.Column(c).Grow(1).Padding(14).Gap(3).Radius(uikit.PanelRadius).
		Background(theme.Surface).Border(1, theme.Border).Children(func() {
		ui.Text(c, label).FontSize(11).TextColor(theme.TextMuted)
		ui.Text(c, value).FontSize(24).Bold().TextColor(theme.Accent)
		ui.Text(c, detail).FontSize(10).TextColor(theme.TextMuted).SingleLine()
	})
}

func badge(c *ui.Context, text string, color ui.Color) {
	ui.Row(c).Padding(2, 7).Radius(8).Background(ui.RGBA(color.R, color.G, color.B, 0.12)).
		Children(func() {
			ui.Text(c, text).FontSize(10).Bold().TextColor(color)
		})
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "—"
	}
	return value.Local().Format("2006-01-02 15:04:05")
}

func messageSummary(message kafka.Message) string {
	value := compactText(message.ValueText(), 90)
	if value == "" {
		value = "(空消息)"
	}
	return value
}

func headersDisplay(headers []kafka.Header) string {
	if len(headers) == 0 {
		return "—"
	}
	lines := make([]string, 0, len(headers))
	for _, header := range headers {
		lines = append(lines, fmt.Sprintf("%s: %s", header.Key, kafka.DisplayBytes(header.Value)))
	}
	return strings.Join(lines, "\n")
}

func filterTopics(items []kafka.TopicSummary, filter string) []kafka.TopicSummary {
	filter = strings.ToLower(strings.TrimSpace(filter))
	if filter == "" {
		return items
	}
	out := make([]kafka.TopicSummary, 0, len(items))
	for _, item := range items {
		if strings.Contains(strings.ToLower(item.Name), filter) {
			out = append(out, item)
		}
	}
	return out
}

func keySearchPlaceholder(substringMatch bool) string {
	if substringMatch {
		return ""
	}
	return "完整 Key"
}

func filterGroups(items []kafka.GroupSummary, filter string, activeOnly bool) []kafka.GroupSummary {
	filter = strings.ToLower(strings.TrimSpace(filter))
	if filter == "" && !activeOnly {
		return items
	}
	out := make([]kafka.GroupSummary, 0, len(items))
	for _, item := range items {
		if activeOnly && item.MemberCount == 0 {
			continue
		}
		if strings.Contains(strings.ToLower(item.GroupID), filter) || strings.Contains(strings.ToLower(item.State), filter) {
			out = append(out, item)
		}
	}
	return out
}

func filterTopicOptions(options []string, query string) []string {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return options
	}
	out := make([]string, 0, len(options))
	for _, option := range options {
		if strings.HasPrefix(strings.ToLower(option), query) {
			out = append(out, option)
		}
	}
	return out
}

func messageDetail(c *ui.Context, message kafka.Message) {
	theme := c.Theme()
	ui.Column(c).FillWidth().Gap(10).Children(func() {
		ui.Row(c).FillWidth().Gap(8).Children(func() {
			badge(c, message.Topic, theme.Accent)
			badge(c, fmt.Sprintf("Partition %d", message.Partition), theme.Success)
			badge(c, fmt.Sprintf("Offset %d", message.Offset), theme.TextMuted)
		})
		ui.Text(c, formatTime(message.Timestamp)).FontSize(11).TextColor(theme.TextMuted)
		ui.Text(c, "Key").FontSize(10).Bold().TextColor(theme.TextMuted)
		value := message.KeyText()
		ui.TextArea(c, &value).ReadOnly(true).Lines(1, 3).FillWidth().Font("monospace").FontSize(11)
		ui.Text(c, "Value").FontSize(10).Bold().TextColor(theme.TextMuted)
		detail := message.ValueText()
		ui.TextArea(c, &detail).ReadOnly(true).Lines(4, 12).FillWidth().Font("monospace").FontSize(11)
		ui.Text(c, "Headers").FontSize(10).Bold().TextColor(theme.TextMuted)
		headers := headersDisplay(message.Headers)
		ui.TextArea(c, &headers).ReadOnly(true).Lines(2, 6).FillWidth().Font("monospace").FontSize(11)
	})
}
