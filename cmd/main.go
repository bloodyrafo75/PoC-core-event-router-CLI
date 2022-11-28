package main

import (
	"os"
	"poc-core-event-router/internals/eventRouterCli"

	"github.com/joho/godotenv"
)

var (
	PUBSUB_HOST string
	PROJECT_ID  string
	TOPIC_NAME  string
	PORT        string
)

func main() {
	err := getEnvConfiguration()
	if err != nil {
		panic(err)
	}

	eventRouterCli.Start(PUBSUB_HOST, PROJECT_ID, TOPIC_NAME, PORT)

}

// get configuration from .env file.
func getEnvConfiguration() error {
	err := godotenv.Load("configs/.env")

	if err != nil {
		return err
	}

	PUBSUB_HOST = os.Getenv("PUBSUB_HOST")
	PROJECT_ID = os.Getenv("PROJECT_ID")
	TOPIC_NAME = os.Getenv("TOPIC_NAME")
	PORT = os.Getenv("PORT")

	return nil
}
