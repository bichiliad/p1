// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
)

type client struct {
	send     chan *Message
	receive  chan *Message
	connect  chan struct{}
	shutdown chan struct{}
	conn     *lspnet.UDPConn
	connId   int
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").

func NewClient(hostport string, params *Params) (Client, error) {
	// Format address
	hostAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, errors.New(fmt.Sprint("Could not resolve UDP address", hostport))
	}

	// Attempt to dial address
	conn, err := lspnet.DialUDP("udp", nil, hostAddr)
	if err != nil {
		return nil, errors.New("UDP Dial Error")
	}

	c := &client{
		send:     make(chan *Message), // For outgoing messages
		receive:  make(chan *Message), // For incoming messages
		connect:  make(chan struct{}), // To signify a successful connection
		shutdown: make(chan struct{}), // To signify a shutdown
		conn:     conn,                // The dialed UDP connection
		connId:   -1,                  // ID of the connection. Shouldn't be -1 tho. lol.
	}

	// Start Master, Net, and Epoch.
	go c.masterHandler()
	go c.netHandler()
	go c.epochHandler()

	// Send a connect message, block until acknowledged
	c.send <- NewConnect()
	<-c.connect

	return nil, errors.New("not yet implemented")
}

func (c *client) ConnID() int {
	return c.connId
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return nil, errors.New("not yet implemented")
}

func (c *client) Write(payload []byte) error {
	return errors.New("not yet implemented")
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}

//
// Private Functions
//

func (c *client) masterHandler() {
	for {
		select {
		case msg := <-c.receive:
			fmt.Println("Received: ")
			fmt.Println(msg.String())
			if msg.Type == MsgAck {
				close(c.connect)
			}
		case msg := <-c.send:
			fmt.Println("Sent: ")
			fmt.Println(msg.String())
			c.sendMessage(msg)
			// Shutdown case
		}

	}
}

func (c *client) netHandler() {
	var buf [1024]byte             // Create buffer
	n, err := c.conn.Read(buf[0:]) // Block on read
	if err != nil {
		fmt.Println("Error reading, connection probably closed")
	}

	// Unmarshal
	var msg Message
	json.Unmarshal(buf[0:n], &msg)
	c.receive <- &msg
}

func (c *client) epochHandler() {
	select {} // lol nothing
}

func (c *client) sendMessage(msg *Message) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return errors.New("Error marshalling")
	}
	fmt.Println(string(bytes))
	c.conn.Write(bytes)

	return nil
}
