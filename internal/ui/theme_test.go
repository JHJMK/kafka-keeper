package ui

import (
	"testing"

	myui "github.com/egoist/mygo/ui"
)

func TestThemeUsesApprovedSkyBluePaletteAndRoundedControls(t *testing.T) {
	theme := Theme()

	if got := theme.Background; got != myui.Hex(PageBackground) {
		t.Fatalf("background = %#v, want %s", got, PageBackground)
	}
	if got := theme.Surface; got != myui.Hex(SurfaceColor) {
		t.Fatalf("surface = %#v, want %s", got, SurfaceColor)
	}
	if got := theme.Accent; got != myui.Hex(AccentColor) {
		t.Fatalf("accent = %#v, want %s", got, AccentColor)
	}
	if got := theme.Radius; got != ControlRadius {
		t.Fatalf("control radius = %v, want %v", got, ControlRadius)
	}
	if ControlRadius < 8 || PanelRadius < 12 || WindowRadius < 20 {
		t.Fatalf("unsafe radius set: control=%v panel=%v window=%v", ControlRadius, PanelRadius, WindowRadius)
	}
}
