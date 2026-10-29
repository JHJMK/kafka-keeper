package app

import (
	"github.com/egoist/mygo"
	myui "github.com/egoist/mygo/ui"

	uikit "github.com/jjk/kafka-keeper/internal/ui"
)

func (a *App) Start() {
	mygo.App.WhenReady(func() {
		win := mygo.NewWindow(mygo.WindowOptions{
			Title:           "Kafka Keeper",
			Width:           1440,
			Height:          900,
			MinWidth:        1100,
			MinHeight:       720,
			StateKey:        "main",
			BackgroundColor: uikit.PageBackground,
			CornerRadius:    uikit.WindowRadius,
			Content:         myui.View(a.View()),
		})
		a.SetWindow(win)
		win.OnClose(func(*mygo.CloseEvent) {
			a.Close()
		})
		a.RefreshAll()
	})
}
