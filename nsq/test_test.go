package nsq

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
)

const (
	// 用于测试的消息主题
	testTopic = "my_test_topic"
	// nsqd 服务端地址
	nsqdAddr = "192.168.117.3:4150"
	// 订阅 channelGroupA 的消费者 A1
	consumerA1 = "consumer_a1"
	// 订阅 channelGroupA 的消费者 A2
	consumerA2 = "consumer_a2"
	// 单独订阅 channelGroupB 的消费者 B
	consumerB = "consumer_b"
	// testTopic 下的 channelA
	channelGroupA = "channel_a"
	// testTopic 下的 channelB
	channelGroupB = "channel_b"
)

// 建立 consumer 与 channel 之间的映射关系
// consumerA1、consumerA2 -> channelA
// consumerB -> channelB
var consumerToChannelGroup = map[string]string{
	consumerA1: channelGroupA,
	consumerA2: channelGroupA,
	consumerB:  channelGroupB,
}

func Test_nsq(t *testing.T) {
	// 消费者消费到消息后的执行逻辑
	msgCallback := func(consumerName string, msg []byte) error {
		t.Logf("i am %s, receive msg: %s", consumerName, msg)
		return nil
	}

	// 运行 producer
	if err := runProducer(); err != nil {
		t.Error(err)
		return
	}

	// 并发运行三个 consumer
	var wg sync.WaitGroup
	for consumer := range consumerToChannelGroup {
		// shadow
		wg.Add(1)
		go func(consumer string) {
			defer wg.Done()
			if err := runConsumer(consumer, msgCallback); err != nil {
				t.Error(err)
			}
		}(consumer)

	}
	wg.Wait()
}

// 运行生产者
func runProducer() error {
	// 通过 addr 直连 nsqd 服务端
	producer, err := nsq.NewProducer(nsqdAddr, nsq.NewConfig())
	if err != nil {
		return err
	}
	// defer producer.Stop()

	// 通过 producer.Publish 方法，往 testTopic 中发送三条消息
	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("hello alphaMinZ %d", i)
		if err := producer.Publish(testTopic, []byte(msg)); err != nil {
			return err
		}
	}
	return nil
}

// 用于处理消息的 processor，需要实现 go-nsq 中定义的 msgProcessor interface，核心是实现消息回调处理方法： func HandleMessage(msg *nsq.Message) error
type msgProcessor struct {
	// 消费者名称
	consumerName string
	// 消息回调处理函数
	callback func(consumerName string, msg []byte) error
}

func newMsgProcessor(consumerName string, callback func(consumerName string, msg []byte) error) *msgProcessor {
	return &msgProcessor{
		consumerName: consumerName,
		callback:     callback,
	}
}

// 消息回调处理
func (m *msgProcessor) HandleMessage(msg *nsq.Message) error {
	// 执行用户定义的业务处理逻辑
	if err := m.callback(m.consumerName, msg.Body); err != nil {
		return err
	}
	// 倘若业务处理成功，则调用 Finish 方法，发送消息的 ack
	msg.Finish()
	return nil
}

// 运行消费者
func runConsumer(consumerName string, callback func(consumerName string, msg []byte) error) error {
	// 根据消费者名获取到对应的 channel
	channel, ok := consumerToChannelGroup[consumerName]
	if !ok {
		return fmt.Errorf("bad name: %s", consumerName)
	}

	// 指定 topic 和 channel，创建 consumer 实例
	consumer, err := nsq.NewConsumer(testTopic, channel, nsq.NewConfig())
	if err != nil {
		return err
	}
	defer consumer.Stop()

	// 添加消息回调处理函数
	consumer.AddHandler(newMsgProcessor(consumerName, callback))

	// consumer 连接到 nsqd 服务端，开启消费流程
	if err = consumer.ConnectToNSQD(nsqdAddr); err != nil {
		return err
	}

	<-time.After(5 * time.Second)

	return nil
}
