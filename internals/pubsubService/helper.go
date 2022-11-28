package pubsubService

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
)

const TOPIC_ALREADY_EXISTS_MSG = "rpc error: code = AlreadyExists desc = Topic already exists"

func getTopic(topicName string, pubsubClient pubsub.Client) (*pubsub.Topic, error) {
	topic, err := pubsubClient.CreateTopic(context.Background(), topicName)

	if err != nil && err.Error() != TOPIC_ALREADY_EXISTS_MSG {
		return nil, err
	}

	if err != nil && err.Error() == TOPIC_ALREADY_EXISTS_MSG {
		topic = pubsubClient.Topic(topicName)
		if topic == nil {
			return nil, fmt.Errorf("topic %s doesn't exist", topicName)
		}
	}
	return topic, nil
}

func getPubsubClient(ctx context.Context, pubsubHost string, projectId string) (*pubsub.Client, error) {
	// Set PUBSUB_EMULATOR_HOST environment variable.
	err := os.Setenv("PUBSUB_EMULATOR_HOST", pubsubHost)
	if err != nil {
		fmt.Println(err)
	}

	// Create client as usual.
	// TODO set connection timeout
	pubsubClient, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		return nil, err
	}
	return pubsubClient, nil
}

func GetPubsubConnectionToTopic(ctx context.Context, host string, projectId string, topicName string) (*pubsub.Client, *pubsub.Topic) {
	log.Println("Connecting to Pubsub:", host)
	pubsubClient, err := getPubsubClient(ctx, host, projectId)
	if err != nil {
		log.Println("Error connecting to Pubsub:", err)
		return nil, nil
	}

	topic, err := getTopic(topicName, *pubsubClient)
	if err != nil {
		return pubsubClient, nil
	}
	log.Println("Connected to Pubsub:", host)

	return pubsubClient, topic
}
