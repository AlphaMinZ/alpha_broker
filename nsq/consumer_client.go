package nsq

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/nsqio/go-nsq"
)

type ConsumerConfig struct {
	Topic        string
	Channel      string
	Address      string
	Lookup       []string
	MaxInFlight  int
	Identify     nsq.IdentifyResponse
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	DrainTimeout time.Duration
}

func (c *ConsumerConfig) defaults() {
	if c.MaxInFlight == 0 {
		c.MaxInFlight = DefaultMaxInFlight
	}

	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}

	if c.ReadTimeout == 0 {
		c.ReadTimeout = DefaultReadTimeout
	}

	if c.WriteTimeout == 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}
}

type ConsumerClient struct {
	q                *nsq.Consumer
	messagesSent     int
	messagesReceived int
	messagesFailed   int
	NSQDAddresses    []string
}

func NewConsumerClient(conf ConsumerConfig, cb func(c *nsq.Config)) *ConsumerClient {
	config := nsq.NewConfig()
	config.LocalAddr, _ = net.ResolveTCPAddr("tcp", conf.Address+":0")
	//config.DefaultRequeueDelay = 0
	//config.MaxBackoffDuration = time.Millisecond * 50
	if cb != nil {
		cb(config)
	}
	if config.Deflate {
		conf.Topic = conf.Topic + "_deflate"
	} else if config.Snappy {
		conf.Topic = conf.Topic + "_snappy"
	}
	if config.TlsV1 {
		conf.Topic = conf.Topic + "_tls"
	}
	q, _ := nsq.NewConsumer(conf.Topic, conf.Channel, config)
	q.SetLogger(log.Default(), nsq.LogLevelDebug)

	c := &ConsumerClient{q: q}
	c.NSQDAddresses = conf.Lookup
	return c
}

func (c *ConsumerClient) AddHandle(handler nsq.Handler) {
	c.q.AddHandler(handler)
	err := c.q.ConnectToNSQDs(c.NSQDAddresses)
	if err != nil {
		fmt.Println(err)
	}
}
