package nsq

import (
	"log"
	"time"

	"github.com/nsqio/go-nsq"
)

type ProducerConfig struct {
	FailOnConnErr  bool
	MaxConcurrency int
	Address        string
	Topic          string
	DialTimeout    time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

func (c *ProducerConfig) defaults() {
	if len(c.Address) == 0 {
		c.Address = "localhost:4151"
	}

	if c.MaxConcurrency == 0 {
		c.MaxConcurrency = DefaultMaxConcurrency
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
}

type ProducerClient struct {
	p         *nsq.Producer
	chStop    chan struct{}
	chMessage chan *PublishData
}

type PublishData struct {
	TopicName string
	Body      []byte
}

func NewProducerClient(addr string) *ProducerClient {
	config := nsq.NewConfig()

	w, _ := nsq.NewProducer(addr, config)
	w.SetLogger(log.Default(), nsq.LogLevelInfo)
	return &ProducerClient{
		p:         w,
		chStop:    make(chan struct{}),
		chMessage: make(chan *PublishData, 1),
	}
}

func (c *ProducerClient) Run() {
	for {
		select {
		case <-c.chStop:
			c.p.Stop()
		case data := <-c.chMessage:
			err := c.p.Publish(data.TopicName, data.Body)
			if err != nil {
				// TODO
			}
		}
	}
}

func (c *ProducerClient) Pub(topicName string, body []byte) {
	c.chMessage <- &PublishData{
		TopicName: topicName,
		Body:      body,
	}
}
