package example

import (
	"fmt"
	"testing"
	"time"

	"github.com/AlphaMinZ/alpha_broker/nsq"
	gnsq "github.com/nsqio/go-nsq"
)

func TestConsumer(t *testing.T) {
	conf := nsq.ConsumerConfig{
		Topic:   "test",
		Channel: "tch",
		Address: "192.168.117.3",
		Lookup:  []string{"192.168.117.3:4150"},
	}
	consumerClient := nsq.NewConsumerClient(conf, nil)
	ch := &ConsumerHandle{}
	ch.Register(0, func(message *gnsq.Message) error {
		fmt.Println("hello nsq")
		return nil
	})
	consumerClient.AddHandle(ch)
	// select {}
	<-time.After(5 * time.Second)
}
