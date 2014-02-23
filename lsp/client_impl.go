// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
)

const (
	bufferSize        = 1024
	channelBufferSize = 40
)

type client struct {
	send          chan *Message
	receive       chan *Message
	connect       chan struct{}
	requestSeqNum chan int
	shutdown      chan struct{}
	read          *UChannel
	write         *UChannel
	conn          *lspnet.UDPConn
	connId        int
	params        *Params
	toWrite       *list.List
	sWindow       *SendWindow
	rWindow       *ReceiveWindow
	seqNum        int
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
		send:          make(chan *Message, channelBufferSize), // For outgoing messages
		receive:       make(chan *Message, channelBufferSize), // For incoming messages
		connect:       make(chan struct{}),                    // To signify a successful connection
		requestSeqNum: make(chan int),                         // For requesting seqnum
		shutdown:      make(chan struct{}),                    // To signify a shutdown
		read:          NewUnboundedChannel(),                  // For queueing messages to Read()
		conn:          conn,                                   // The dialed UDP connection
		connId:        -1,                                     // ID of the connection. Set on connection.
		params:        params,                                 // Parameters
		toWrite:       list.New(),                             // Queue of yet-to-be-sent messages
		sWindow:       NewSendWindow(params.WindowSize),       // Create new window
		rWindow:       NewReceiveWindow(params.WindowSize),    // Create new window
		seqNum:        0,                                      // Initial sequence number
	}

	// Start Master, Net, and Epoch.
	go c.masterHandler()
	go c.netHandler()
	go c.epochHandler()

	// Send a connect message, block until acknowledged
	c.send <- NewConnect()
	<-c.connect

	return c, nil
}

// Returns connection ID.
func (c *client) ConnID() int {
	return c.connId
}

// Blocks until new message received or connection closes
func (c *client) Read() ([]byte, error) {
	msg := <-c.read.out
	if msg.Type == MsgAck {
		return nil, errors.New("Connection has been closed")
	}
	return msg.Payload, nil
}

// Takes a byte array, packages it into a message, queues it up to be sent
func (c *client) Write(payload []byte) error {
	// request SeqNum
	c.requestSeqNum <- 0
	s := <-c.requestSeqNum
	// create message
	msg := NewData(c.connId, s, payload)

	// Is the write channel open?
	if c.send == nil {
		return errors.New("write channel has been closed")
	} else {

		c.send <- msg
		return nil
	}
}

func (c *client) Close() error {
	close(c.shutdown)
	return errors.New("not yet implemented")
}

//
// Private Functions
//

func (c *client) masterHandler() {
	for {
		select {
		case msg := <-c.receive: // Just got an inbound message, oh boy.
			fmt.Println("Received: ")
			fmt.Println(msg.String())
			// Handle that ish.
			c.onReceive(msg)

		case msg := <-c.send: // Attempting to send message
			fmt.Println("Sent: ")
			fmt.Println(msg.String())

			c.sendMessage(msg)

		case <-c.requestSeqNum: // Request to update/assign a SeqNum
			c.seqNum++
			c.requestSeqNum <- c.seqNum
		case <-c.shutdown: // Shutdown case
			return //TODO: NOT THIS.
		}

	}
}

func (c *client) onReceive(msg *Message) {
	// Handle Acknowledgements
	switch msg.Type {
	case MsgAck:

		// Is it a connection response?
		if msg.SeqNum == 0 && c.ConnID() == -1 {
			c.connId = msg.ConnID   // Set the connection ID
			close(c.connect)        // Close the connection notifier
			c.connect = nil         // REALLY close it. Ha.
			fmt.Println(c.ConnID()) // Print out connection id.
		} else {
			c.sWindow.ack(msg) // Try to add to window
			// Update write queue
			for {
				next := c.toWrite.Front()
				// Is there a next?
				if next == nil || !c.sWindow.inWindow((next.Value).(*Message)) {
					break
				} else { // Push it into the write channel, remove it from the queue
					c.send <- (next.Value).(*Message)
					c.toWrite.Remove(next)
				}
			}
		}

	case MsgData:
		// Add to received window
		c.rWindow.receive(msg)
		// Update read queue
		for {
			readMsg, ok := c.rWindow.readNext()
			if !ok { // Nothing for now
				break
			} else { // New message to read!
				c.read.in <- readMsg
			}
		}

	default:
		fmt.Println("Whoa, shouldn't be getting here (received a Connect message to client)")
	}
}

func (c *client) netHandler() {
	var buf [bufferSize]byte       // Create buffer
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
	select {} // lol nothing yet
}

//
// Helpers
//

func (c *client) sendMessage(msg *Message) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return errors.New("Error marshalling")
	}
	fmt.Println(string(bytes))
	c.conn.Write(bytes)

	return nil
}
