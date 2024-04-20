package example

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AlphaMinZ/alpha_broker/nsq"
	gnsq "github.com/nsqio/go-nsq"
)

func TestProducer(t *testing.T) {
	client := nsq.NewProducerClient("192.168.117.3:4150")
	go client.Run()
	msgIDGood := gnsq.MessageID{'1'}
	msgGood := gnsq.NewMessage(msgIDGood, []byte("good"))
	bytes, _ := json.Marshal(msgGood)
	client.Pub("test", bytes)
	// select {}
	<-time.After(5 * time.Second)
}
