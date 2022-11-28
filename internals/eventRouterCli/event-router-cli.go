package eventRouterCli

import (
	"context"
	"fmt"
	"poc-core-event-router/internals/pubsubService"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
)

func GetConnection() error {
	return nil

}

func Start(host string, projectId string, topicName string, PORT string) error {
	ctx := context.Background()
	pubsubClient, topic := pubsubService.GetPubsubConnectionToTopic(ctx, host, projectId, topicName)

	timeStamp := time.Now().Unix()
	subscriptionName := "subscription_" + strconv.FormatInt(timeStamp, 10)

	sub := pubsubClient.Subscription(subscriptionName)
	ok, err := sub.Exists(ctx)
	if err != nil {
		fmt.Println("ERROR RETRIEVING THE SUBSCRIPTION")
	}
	if !ok {
		sub, err = pubsubClient.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{
			Topic:            topic,
			AckDeadline:      20 * time.Second,
			ExpirationPolicy: 25 * time.Hour,
		})
		if err != nil {
			fmt.Println("Error creating subscriptor")
			fmt.Println(err)
		}
	}

	fmt.Println("we have the Subscription ")
	fmt.Println(sub)

	//test permissions
	//testPermissions(PROJECT_ID, TOPIC_NAME)
	pullMsgs(ctx, *pubsubClient, subscriptionName)

	//gorountines are cleaned up
	topic.Stop()
	//close connection
	defer pubsubClient.Close()

	return nil

}

func pullMsgs(ctx context.Context, client pubsub.Client, subID string) error {
	sub := client.Subscription(subID)
	//ctx, cancel := context.WithTimeout(ctx, 10*time.Second) //will listen only for 10.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := sub.Receive(cctx, func(_ context.Context, msg *pubsub.Message) {
		msg.Ack()
		fmt.Println("Got message ("+subID+"): ", string(msg.ID), string(msg.ID), string(msg.Data))
		fmt.Println("Got attributes ("+subID+"): ", msg.Attributes)
	})
	if err != nil {
		return fmt.Errorf("sub.Receive: %v", err)
	}

	return nil
}
