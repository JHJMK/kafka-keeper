package ui

import myui "github.com/egoist/mygo/ui"

const (
	PageBackground = "#F5FBFE"
	SidebarTop     = "#EDF9FF"
	SidebarBottom  = "#F7FCFF"
	SurfaceColor   = "#FFFFFF"
	AccentColor    = "#269BDC"
	AccentHover    = "#5BC3F4"
	TextColor      = "#24455A"
	TextMutedColor = "#7797A9"
	BorderColor    = "#DCECF6"
	SuccessColor   = "#2E9D74"
	DangerColor    = "#D85B67"

	ControlRadius = 8
	PanelRadius   = 14
	WindowRadius  = 22
)

func Hex(value string) myui.Color {
	return myui.Hex(value)
}

func Theme() *myui.Theme {
	base := *myui.LightTheme()
	base.Background = Hex(PageBackground)
	base.Surface = Hex(SurfaceColor)
	base.SurfaceHover = Hex(SidebarBottom)
	base.SurfacePressed = Hex(SidebarTop)
	base.Border = Hex(BorderColor)
	base.Text = Hex(TextColor)
	base.TextMuted = Hex(TextMutedColor)
	base.Accent = Hex(AccentColor)
	base.AccentHover = Hex(AccentHover)
	base.AccentPressed = Hex(AccentColor)
	base.AccentText = Hex(SurfaceColor)
	base.Success = Hex(SuccessColor)
	base.Danger = Hex(DangerColor)
	base.Selection = myui.RGBA(38, 155, 220, 0.18)
	base.Focus = myui.RGBA(38, 155, 220, 0.55)
	base.Scrollbar = myui.RGBA(119, 151, 169, 0.42)
	base.Radius = ControlRadius
	base.Spacing = 5
	return &base
}
