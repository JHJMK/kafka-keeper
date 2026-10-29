package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/egoist/mygo"

	"github.com/jjk/kafka-keeper/app"
	"github.com/jjk/kafka-keeper/internal/kafka"
	"github.com/jjk/kafka-keeper/internal/store"
)

const userDataDirName = "kafka-keeper"

func main() {
	mygo.App.SetName("Kafka Keeper")
	dataDir, err := configureUserDataDir()
	if err != nil {
		log.Fatal(err)
	}
	credentials := store.NewCredentials(store.NewFileSecrets(filepath.Join(dataDir, "credentials.json")))
	application := app.NewWithDependencies(app.Dependencies{
		Connections: store.NewFileStore(filepath.Join(dataDir, "connections.json")),
		Credentials: credentials,
		Kafka:       kafka.NewService(credentials),
	})
	mygo.App.OnQuit(application.Close)
	application.Start()
	if err := mygo.App.Run(); err != nil {
		log.Fatal(err)
	}
}

func configureUserDataDir() (string, error) {
	homeDir, err := mygo.App.Path(mygo.PathHome)
	if err != nil {
		return "", err
	}
	dataDir := filepath.Join(homeDir, userDataDirName)
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return "", err
	}
	mygo.App.SetPath(mygo.PathUserData, dataDir)
	return dataDir, nil
}
