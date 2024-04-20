package nsq

import (
	"time"
)

const (
	DefaultUserAgent       = ""
	DefaultMaxConcurrency  = 1
	DefaultMaxInFlight     = 1
	DefaultDialTimeout     = 5 * time.Second
	DefaultReadTimeout     = 1 * time.Minute
	DefaultWriteTimeout    = 10 * time.Second
	DefaultLookupTimeout   = 10 * time.Second
	DefaultMaxRetryTimeout = 10 * time.Second
	DefaultMinRetryTimeout = 10 * time.Millisecond
	DefaultDrainTimeout    = 10 * time.Second

	NoTimeout = time.Duration(0)
)

type NodeData struct {
	RemoteAddr    string      `json:"remote_address"`
	HostName      string      `json:"hostname"`
	BroadcastAddr string      `json:"broadcast_address"`
	TCPPort       int         `json:"tcp_port"`
	HTTPPort      int         `json:"http_port"`
	Version       string      `json:"version"`
	Tombstones    interface{} `json:"tombstones"`
	Topics        interface{} `json:"topics"`
}

type NodesData struct {
	Producers []*NodeData `json:"producers"`
}

const (
	httpPrefix    = "http://"
	createTopic   = httpPrefix + "%s" + "/topic/create?topic=" + "%s"
	deleteTopic   = httpPrefix + "%s" + "/topic/delete?topic=" + "%s"
	createChannel = httpPrefix + "%s" + "/channel/create?topic=" + "%s" + "&channel=" + "%s"
	deleteChannel = httpPrefix + "%s" + "/channel/delete?topic=" + "%s" + "&channel=" + "%s"
	queryNodeData = httpPrefix + "%s/nodes"
)
