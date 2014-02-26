// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"io/ioutil"
	"log"
	// "os"
	"time"
)

const (
	bufferSize         = 1024
	channelBufferSize  = 100
	readSeqNum         = 0
	readConnId         = 1
	gracefulShutdown   = 2
	disconnectShutdown = 3
	gracefulComplete   = 4
)

var LOGE = log.New(ioutil.Discard, "  [ERROR]   ", log.Lmicroseconds|log.Lshortfile)

// var LOGV = log.New(os.Stdout, "  [VERBOSE] ", log.Lmicroseconds|log.Lshortfile)

var LOGV = log.New(ioutil.Discard, "  [VERBOSE] ", log.Lmicroseconds|log.Lshortfile)

type client struct {
	send             chan *Message
	receive          chan *Message
	connect          chan struct{}
	requestData      chan int
	shutdown         chan int
	shutdownComplete chan int
	kill             chan int
	read             *UChannel
	write            *UChannel
	conn             *lspnet.UDPConn
	connId           int
	params           *Params
	toWrite          *list.List
	sWindow          *SendWindow
	rWindow          *ReceiveWindow
	seqNum           int
	closing          bool
	closeCounter     int
	gotMail          bool
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
		LOGE.Printf("Could not resolve UDP address", hostport)
		return nil, errors.New(fmt.Sprint("Could not resolve UDP address", hostport))
	}

	// Attempt to dial address
	conn, err := lspnet.DialUDP("udp", nil, hostAddr)
	if err != nil {
		LOGE.Printf("UDP Dial Error: ", err)
		return nil, errors.New("UDP Dial Error")
	}

	c := &client{
		send:             make(chan *Message, channelBufferSize), // For outgoing messages
		receive:          make(chan *Message, channelBufferSize), // For incoming messages
		connect:          make(chan struct{}),                    // To signify a successful connection
		requestData:      make(chan int),                         // For requesting data synchronously
		shutdown:         make(chan int, 1),                      // To signal a shutdown to the master channel
		shutdownComplete: make(chan int),                         // To tell Close() to unblock
		kill:             make(chan int),                         // To alert goroutines to return
		read:             NewUnboundedChannel(),                  // For queueing messages to Read()
		conn:             conn,                                   // The dialed UDP connection
		connId:           -1,                                     // ID of the connection. Set on connection.
		params:           params,                                 // Parameters
		toWrite:          list.New(),                             // Queue of yet-to-be-sent messages
		sWindow:          NewSendWindow(params.WindowSize),       // Create new window
		rWindow:          NewReceiveWindow(params.WindowSize),    // Create new window
		seqNum:           0,                                      // Initial sequence number
		closing:          false,                                  // Are we trying to close right now?
		closeCounter:     0,                                      // Epochs since last message
		gotMail:          false,                                  // Did we get a message this epoch
	}

	// Start Master, Net, and Epoch.
	go c.masterHandler()
	go c.netHandler()
	go c.epochHandler()

	// Send a connect message, block until acknowledged
	c.send <- NewConnect()
	<-c.connect // Connected!
	if c.connId == -1 {
		return nil, errors.New("Unable to connect")
	}

	LOGV.Printf("Started connection with ConnID %d", c.connId)
	return c, nil
}

// Returns connection ID.
func (c *client) ConnID() int {
	c.requestData <- readConnId //FIXME: Is this lock contentioius? Multiple signals in one channel?
	id := <-c.requestData
	return id
}

// Blocks until new message received or connection closes
func (c *client) Read() ([]byte, error) {
	if c.read == nil {
		LOGE.Println("Connection has been closed")
		return nil, errors.New("Connection has been closed")
	}
	msg := <-c.read.out
	if msg == nil || msg.Type == MsgAck { // TODO: Maybe just checkif c.read.out == nil
		LOGE.Println("Connection has been closed")
		return nil, errors.New("Connection has been closed")
	}
	return msg.Payload, nil
}

// Takes a byte array, packages it into a message, queues it up to be sent
func (c *client) Write(payload []byte) error {
	// request SeqNum
	c.requestData <- readSeqNum
	s := <-c.requestData
	// create message
	msg := NewData(c.connId, s, payload)

	// Is the write channel open?
	if c.send == nil {
		LOGE.Println("Write channel has been closed")
		return errors.New("write channel has been closed")
	} else {
		// TODO: Move to inside master
		if c.sWindow.inWindow(msg) {
			LOGV.Println("Writing directly")
			c.sWindow.add(msg)
			c.send <- msg
		} else {
			LOGV.Println("Queueing message")
			c.toWrite.PushBack(msg)
		}
		return nil
	}
}

func (c *client) Close() error {
	defer LOGV.Println("Shutdown - Close() has unblocked")
	c.shutdown <- gracefulShutdown
	LOGV.Println("Shutdown - waiting for shutdown")
	<-c.shutdownComplete
	return nil
}

//
// Private Functions
//

func (c *client) masterHandler() {
	defer LOGV.Println("Shutdown - masterhandler closing now")

	tick := time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
	ticker := tick.C

	for {
		select {
		case <-ticker:
			c.epochHandler()
		case msg := <-c.receive: // Just got an inbound message, oh boy.
			c.gotMail = true // We've got mail.
			LOGV.Printf(fmt.Sprint("Received: ", msg.String()))
			// Handle that ish.
			c.onReceive(msg)

			// Check if we should return. If there's a data msg in the window, we must wait on it
			if c.closing && c.sWindow.yetToSend() == 0 && c.toWrite.Len() == 0 {
				LOGV.Println("Done waiting on everything")
				close(c.kill)             // Kill net handler
				tick.Stop()               // Kill the ticker
				c.conn.Close()            // Close the connection
				close(c.shutdownComplete) // Tell Close() that we're done
				return
			}

		case msg := <-c.send: // Attempting to send message
			if msg == nil {
				LOGE.Println("Tried to send Nil message")
			} else {
				LOGV.Printf(fmt.Sprint("Sent: ", msg.String()))
				c.sendMessage(msg)
			}

		case flag := <-c.requestData: // Request to update/assign a SeqNum
			LOGV.Println("Request for data")
			switch flag {
			case readSeqNum:
				LOGV.Println("Requesting seq num update")
				c.seqNum++
				c.requestData <- c.seqNum
			case readConnId:
				LOGV.Println("Requesting connId")
				c.requestData <- c.connId
			}

		case flag := <-c.shutdown: // Shutdown case
			if flag == gracefulShutdown {
				// Set the state to closing.
				c.closing = true
				// Flush the read queue as no more Read()'s will be called
				c.read.CloseIn()
				for _ = range c.read.out {
					// Do nothing, empty the queue. All those poor, poor reads.
				}

			} else if flag == disconnectShutdown {
				LOGS.Println("Disconnectshutdown start")
				// Do it incorrectly for now
				close(c.kill)
				c.conn.Close()
				LOGS.Println("Disconnectshutdown killed")
				c.read.CloseIn() // Close read input, leave output alone.
				LOGS.Println("Disconnectshutdown done")
				return

			}
		}

	}
}

func (c *client) onReceive(msg *Message) {
	// Handle Acknowledgements
	switch msg.Type {
	case MsgAck:

		// Is it a connection response?
		if msg.SeqNum == 0 && c.connId == -1 {
			LOGV.Println("Got connection response")
			c.rWindow.window[0] = msg // Acknowledge id 0 so that we have something to send at epochs
			c.connId = msg.ConnID     // Set the connection ID
			close(c.connect)          // Close the connection notifier
			LOGV.Println(c.connId)    // Print out connection id.
		} else if msg.SeqNum != 0 { // Ignore reconfirmations of connection
			LOGV.Println(fmt.Sprint("Got acknowledgement ", msg.String()))
			c.sWindow.ack(msg) // Try to add to window

			// Update write queue
			for {
				next := c.toWrite.Front()
				// Is there a next?
				if next == nil || !c.sWindow.inWindow((next.Value).(*Message)) {
					break
				} else { // Push it into the write channel, add to the sendWindow, remove it from the queue
					c.sWindow.add((next.Value).(*Message))
					c.send <- (next.Value).(*Message)
					c.toWrite.Remove(next)
				}
			}
		}

	case MsgData:
		// Add to received window
		c.rWindow.receive(msg)
		// Acknowledge that we received the message
		c.send <- NewAck(msg.ConnID, msg.SeqNum)
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
		LOGE.Println("Whoa, shouldn't be getting here (received a Connect message to client)")
	}
}

func (c *client) netHandler() {
	defer LOGV.Println("Shutdown - net handler closed")

	var buf [bufferSize]byte // Create buffer
	for {
		select {
		case <-c.kill:
			return

		default:
			n, err := c.conn.Read(buf[0:]) // Block on read
			if err != nil {
				LOGE.Println(fmt.Sprint("Error reading, connection probably closed: ", err))
			} else {

				// Unmarshal
				var msg Message
				json.Unmarshal(buf[0:n], &msg)

				if msg.Type == MsgData {
					LOGE.Println(fmt.Sprint("Client read has occurred: ", msg.String()))
				}
				c.receive <- &msg
			}
		}
	}

}

func (c *client) epochHandler() {

	LOGV.Printf("Client %d epoch", c.connId)
	// check if disconnected
	if c.gotMail == true {
		c.gotMail = false
		c.closeCounter = 0
	} else {
		c.closeCounter++
		LOGV.Printf("Timeout count %d", c.closeCounter)
		if c.closeCounter == c.params.EpochLimit {
			// Waited long enough, should disconnect.
			LOGV.Printf("Timeout error")
			c.shutdown <- disconnectShutdown
		}

	}

	id := c.connId
	if id == -1 { // Need to connect, resend request
		c.send <- NewConnect()
	} else {
		// Resend acks!
		if c.sWindow.base == 1 { // Window hasn't moved, might as well resend connection ack
			c.send <- c.rWindow.window[0] // Must exist, we have a connection id
		}
		// Resend all acks within window from receiveWindow
		for tmp := c.rWindow.base; tmp < c.rWindow.base+c.rWindow.size; tmp++ {
			reack, ok := c.rWindow.window[tmp]
			if ok {
				reack_msg := NewAck(reack.ConnID, reack.SeqNum)
				LOGV.Println(fmt.Sprint("Resending ack: ", reack_msg.String()))
				c.send <- reack_msg
			}
		}

		// Resend messages
		for tmp := c.sWindow.base; tmp < c.sWindow.base+c.sWindow.size; tmp++ {
			resend, ok := c.sWindow.window[tmp]
			// If there's a message there, but there isn't an acknowledgement, resend the message
			if ok && resend.Type == MsgData {
				LOGV.Println(fmt.Sprint("Resending data: ", resend.String()))
				c.send <- resend
			}
		}
	}
}

//
// Helpers
//

func (c *client) sendMessage(msg *Message) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return errors.New("Error marshalling")
	}
	_, err = c.conn.Write(bytes)
	if err != nil {
		LOGE.Println(fmt.Sprint("Error writing, connection is probably dead: ", err))
		return errors.New("Error writing")
	}
	LOGV.Println(fmt.Sprint("Write occurred: ", msg.String()))

	return nil
}
