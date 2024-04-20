package nsq

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

type ManagerConfig struct {
	Configs []*ProducerManagerConfig `json:"configs"`
}

type ProducerManagerConfig struct {
	Category        string            `json:"category"`
	ProducerConfigs []*ProducerConfig `json:"producerConfigs"`
	PoolSize        int
}

var nsqMgr *Manager

type Manager struct {
	producerManagers sync.Map
}

func Initialize(config *ManagerConfig) {
	nsqMgr = &Manager{}
	for _, c := range config.Configs {
		nsqMgr.AddProducerManager(c)
	}
}

func (m *Manager) AddProducerManager(c *ProducerManagerConfig) {
	_, ok := m.producerManagers.Load(c.Category)
	if ok {
		return
	}
	pm := NewProducerManager(c.ProducerConfigs, c.PoolSize)

	m.producerManagers.Store(c.Category, pm)
}

func (m *Manager) delNsqInstance(category string) {
	_, ok := m.producerManagers.Load(category)
	if !ok {
		return
	}
	m.producerManagers.Delete(category)
}

func (m *Manager) getProducerManager(category string) (*ProducerManager, error) {
	pmObj, ok := m.producerManagers.Load(category)
	if !ok {
		return nil, fmt.Errorf("type  err")
	}

	pm, ok := pmObj.(*ProducerManager)

	if !ok {
		return nil, fmt.Errorf("accert err")
	}

	return pm, nil
}

type ProducerManager struct {
	lookups   []string
	producers []*ProducerClient
}

func NewProducerManager(configs []*ProducerConfig, poolSize int) *ProducerManager {
	pm := &ProducerManager{
		lookups:   make([]string, 0, len(configs)),
		producers: make([]*ProducerClient, 0, len(configs)),
	}
	for _, config := range configs {
		pm.lookups = append(pm.lookups, config.Address)
	}
	tcpAddrs := pm.getAvailableTCPAddrs()
	for i := 0; i < poolSize; i++ {
		for _, addr := range tcpAddrs {
			pm.AddProducer(addr)
		}
	}
	return pm
}

func (m *ProducerManager) AddProducer(address string) {
	producerClient := NewProducerClient(address)
	m.producers = append(m.producers, producerClient)
}

func (m *ProducerManager) GetProducer() *ProducerClient {
	var nsqIns *ProducerClient
	pLen := len(m.producers)
	if pLen > 0 {
		randIdx := rand.Intn(pLen)
		nsqIns = m.producers[randIdx]
	}
	return nsqIns
}

func (m *ProducerManager) getAvailableTCPAddrs() []string {
	var NSQDAddrs []string
	for _, lookupAddr := range m.lookups {
		queryURL := fmt.Sprintf(queryNodeData, lookupAddr)
		resp, err := http.Get(queryURL)
		if err != nil {
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			resp.Body.Close()
			continue
		}

		var NSQDs NodesData
		if err := json.Unmarshal(body, &NSQDs); err != nil {
			resp.Body.Close()
			continue
		}

		for _, producer := range NSQDs.Producers {
			addr := fmt.Sprintf("%s:%d", producer.BroadcastAddr, producer.TCPPort)
			NSQDAddrs = append(NSQDAddrs, addr)
		}
		resp.Body.Close()
	}
	return NSQDAddrs
}

func (m *ProducerManager) getAllNSQDHTTPAddrs() []string {
	var NSQDAddrs []string
	for _, lookupAddr := range m.lookups {
		queryURL := fmt.Sprintf(queryNodeData, lookupAddr)
		resp, err := http.Get(queryURL)
		if err != nil {
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			resp.Body.Close()
			continue
		}
		var NSQDs NodesData
		if err = json.Unmarshal(body, &NSQDs); err != nil {
			resp.Body.Close()
			continue
		}

		for _, producer := range NSQDs.Producers {
			addr := fmt.Sprintf("%v:%v", producer.BroadcastAddr, producer.HTTPPort)
			NSQDAddrs = append(NSQDAddrs, addr)
		}
		resp.Body.Close()
	}
	return NSQDAddrs
}

func (m *ProducerManager) createTopic(topic string) error {
	addresses := m.getAllNSQDHTTPAddrs()
	if len(addresses) <= 0 {
		return errors.New("node number is zero")
	}
	for _, addr := range addresses {
		m.createTopicInEveryNSQDNode(topic, addr)
	}
	return nil
}

func (m *ProducerManager) createTopicInEveryNSQDNode(topic string, nodeAddr string) {
	topicUrl := fmt.Sprintf(createTopic, nodeAddr, topic)
	request, err := http.NewRequest("POST", topicUrl, nil)
	var resp *http.Response
	resp, err = http.DefaultClient.Do(request)
	if err != nil {
		//todo
	}
	resp.Body.Close()
}

func (m *ProducerManager) deleteTopic(topic string) error {
	addresses := m.getAllNSQDHTTPAddrs()
	if len(addresses) <= 0 {
		return errors.New("node number is zero")
	}
	for _, addr := range addresses {
		m.deleteTopicInEveryNSQDNode(topic, addr)
	}
	return nil
}

func (m *ProducerManager) deleteTopicInEveryNSQDNode(topic string, nodeAddr string) {
	topicUrl := fmt.Sprintf(deleteTopic, nodeAddr, topic)
	request, err := http.NewRequest("POST", topicUrl, nil)
	var resp *http.Response
	resp, err = http.DefaultClient.Do(request)
	if err != nil {
		// todo
	}
	resp.Body.Close()
}

func (m *ProducerManager) createChannelInEveryNsqdNode(topic, channel, nodeAddr string) {
	channelUrl := fmt.Sprintf(createChannel, nodeAddr, topic, channel)
	request, err := http.NewRequest("POST", channelUrl, nil)
	var resp *http.Response
	resp, err = http.DefaultClient.Do(request)
	if err != nil {
		// todo
	}
	resp.Body.Close()
}

func (m *ProducerManager) createChannel(topic, channel string) error {
	addresses := m.getAllNSQDHTTPAddrs()
	if len(addresses) <= 0 {
		return errors.New("node number is zero")
	}
	for _, addr := range addresses {
		m.createChannelInEveryNsqdNode(topic, channel, addr)
	}
	return nil
}

func (m *ProducerManager) deleteChannel(topic, channel string) error {
	addresses := m.getAllNSQDHTTPAddrs()
	if len(addresses) <= 0 {
		return errors.New("node number is zero")
	}
	for _, addr := range addresses {
		m.deleteChannlInEveryNsqdNode(topic, channel, addr)
	}
	return nil
}

func (ins *ProducerManager) deleteChannlInEveryNsqdNode(topic, channel, nodeAddr string) {
	channelUrl := fmt.Sprintf(deleteChannel, nodeAddr, topic, channel)
	request, err := http.NewRequest("POST", channelUrl, nil)
	var resp *http.Response
	resp, err = http.DefaultClient.Do(request)
	if err != nil {
		// todo
	}
	resp.Body.Close()
}

func PublishAsync(insType string, topic string, data []byte, doneChan chan *nsq.ProducerTransaction) error {
	nsqIns, err := nsqMgr.getProducerManager(insType)
	if err != nil {
		return err
	}
	producer := nsqIns.GetProducer()
	if producer == nil {
		return errors.New("producer do not exist ")
	}
	return producer.p.PublishAsync(topic, data, doneChan)
}

func DeferredPublishAsync(insType string, topic string, data []byte,
	doneChan chan *nsq.ProducerTransaction, delay time.Duration) error {
	nsqIns, err := nsqMgr.getProducerManager(insType)
	if err != nil {
		return err
	}
	producer := nsqIns.GetProducer()
	if producer == nil {
		return errors.New("producer do not exist ")
	}
	return producer.p.DeferredPublishAsync(topic, delay, data, doneChan)
}

func CreateTopic(insType string, topic string) error {
	nsqIns, err := nsqMgr.getProducerManager(insType)
	if err != nil {
		return err
	}

	return nsqIns.createTopic(topic)
}

func DeleteTopic(insType string, topic string) error {
	nsqIns, err := nsqMgr.getProducerManager(insType)
	if err != nil {
		return err
	}
	return nsqIns.deleteTopic(topic)
}

func CreateChannel(insType string, topic, channel string) error {
	nsqIns, err := nsqMgr.getProducerManager(insType)
	if err != nil {
		return err
	}

	return nsqIns.createChannel(topic, channel)
}

func DeleteChannel(insType string, topic, channel string) error {
	nsqIns, err := nsqMgr.getProducerManager(insType)
	if err != nil {
		return err
	}
	return nsqIns.deleteChannel(topic, channel)
}
