package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/egoist/mygo"
)

func TestConfigureUserDataDirUsesHomeKafkaKeeper(t *testing.T) {
	homeDir := t.TempDir()
	mygo.App.SetPath(mygo.PathHome, homeDir)
	t.Cleanup(func() {
		mygo.App.SetPath(mygo.PathHome, "")
		mygo.App.SetPath(mygo.PathUserData, "")
	})

	dataDir, err := configureUserDataDir()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(homeDir, userDataDirName)
	if dataDir != want {
		t.Fatalf("data dir = %q, want %q", dataDir, want)
	}
	info, err := os.Stat(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("%q is not a directory", dataDir)
	}
	configured, err := mygo.App.Path(mygo.PathUserData)
	if err != nil {
		t.Fatal(err)
	}
	if configured != want {
		t.Fatalf("mygo user data dir = %q, want %q", configured, want)
	}
}
