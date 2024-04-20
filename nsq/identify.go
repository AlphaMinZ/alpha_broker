package nsq

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
)

// Identify represents the IDENTIFY command.
type Identify struct {
	// ClientID should be set to a unique identifier representing the client.
	ClientID string `json:"client_id"`

	// Hostname represents the hostname of the client, by default it is set to
	// the value returned by os.Hostname is used.
	Hostname string `json:"hostname"`

	// UserAgent represents the type of the client, by default it is set to
	// nsq.DefaultUserAgent.
	UserAgent string `json:"user_agent"`

	// MessageTimeout can bet set to configure the server-side message timeout
	// for messages delivered to this consumer.  By default it is not sent to
	// the server.
	MessageTimeout time.Duration `json:"message_timeout"`
}

// Name returns the name of the command in order to satisfy the Command
// interface.
func (c Identify) Name() string {
	return "IDENTIFY"
}

// Write serializes the command to the given buffered output, satisfies the
// Command interface.
func (c Identify) Write(w *bufio.Writer) (err error) {
	var data []byte

	if data, err = json.Marshal(Identify{
		ClientID:       c.ClientID,
		Hostname:       c.Hostname,
		UserAgent:      c.UserAgent,
		MessageTimeout: c.MessageTimeout / time.Millisecond,
	}); err != nil {
		return
	}

	if _, err = w.WriteString("IDENTIFY\n"); err != nil {
		err = errors.Wrap(err, "writing IDENTIFY command")
		return
	}

	if err = binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		err = errors.Wrap(err, "writing IDENTIFY body size")
		return
	}

	if _, err = w.Write(data); err != nil {
		err = errors.Wrap(err, "writing IDENTIFY body data")
		return
	}

	return
}

func readIdentify(r *bufio.Reader) (cmd Identify, err error) {
	var body Identify

	if body, err = readIdentifyBody(r); err != nil {
		return
	}

	cmd = Identify{
		ClientID:       body.ClientID,
		Hostname:       body.Hostname,
		UserAgent:      body.UserAgent,
		MessageTimeout: time.Millisecond * body.MessageTimeout,
	}
	return
}

func readIdentifyBody(r *bufio.Reader) (body Identify, err error) {
	var size uint32
	var data []byte

	if err = binary.Read(r, binary.BigEndian, &size); err != nil {
		err = errors.Wrap(err, "reading IDENTIFY body size")
		return
	}

	data = make([]byte, int(size))

	if _, err = io.ReadFull(r, data); err != nil {
		err = errors.Wrap(err, "reading IDENTIFY body data")
		return
	}

	if err = json.Unmarshal(data, &body); err != nil {
		err = errors.Wrap(err, "decoding IDENTIFY body")
		return
	}

	return
}

func setIdentifyDefaults(c Identify) Identify {
	if len(c.UserAgent) == 0 {
		c.UserAgent = "DefaultUserAgent"
	}

	if len(c.Hostname) == 0 {
		c.Hostname, _ = os.Hostname()
	}

	return c
}
