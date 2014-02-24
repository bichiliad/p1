// Contains the implementation of a LSP server.

package lsp

import (
	// "container/list"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

const (
	BUFFSIZE = 1024
)

type server struct {
	clients     map[int]*cli            // Map of all clients
	addrs       map[*lspnet.UDPAddr]int // Maps addresses to connId's
	conn        *lspnet.UDPConn         // UDP connection to listen on
	send        *UChannel               // Channel to send outbound messages on
	receive     chan *clientMessage     // Channel for inbound messages
	params      *Params                 // Configurable parameters
	toRead      *UChannel               // Things waiting to be read
	connIdCount int                     // What's our next connId?
	reqSeqNum   chan int                // Request a new seqNumber atomically
	killClient  chan int                // For closing clients
	shutdown    chan int                // Request a shutdown
}

type cli struct {
	addr         *lspnet.UDPAddr // Client's address
	connId       int             // The ID of their connection
	seqNum       int             // What message they're on
	sWindow      *SendWindow     // Tracks what was sent to/acknowledged by the client
	rWindow      *ReceiveWindow  // Tracks what was received from the client
	gotMail      bool            // Have we received a message since the last epoch?
	timeoutCount int             // How many epochs since we've heard from them?
	toWrite      *list.List      // Messages we're waiting to send to the client
}

type clientMessage struct {
	addr *lspnet.UDPAddr
	msg  Message
}

// var LOGE = log.New(os.Stderr, "  [ERROR]   ", log.Lmicroseconds|log.Lshortfile)
// var LOGV = log.New(ioutil.Discard, "  [VERBOSE] ", log.Lmicroseconds|log.Lshortfile)

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {

	// Start listening
	addr, err := lspnet.ResolveUDPAddr("udp", fmt.Sprint("localhost:", port))
	if err != nil {
		LOGE.Println("Error resolving address: " + fmt.Sprint("localhost:", port))
		return nil, errors.New("Error resolving address: " + fmt.Sprint("localhost:", port))
	}
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		LOGE.Println(fmt.Sprint("Error listening: ", err))
		return nil, errors.New(fmt.Sprint("Error listening: ", err))
	}

	// Package and return server
	s := &server{
		clients:     make(map[int]*cli),
		addrs:       make(map[*lspnet.UDPAddr]int),
		conn:        conn,
		send:        NewUnboundedChannel(),
		receive:     make(chan *clientMessage, channelBufferSize),
		params:      params,
		toRead:      NewUnboundedChannel(),
		connIdCount: 1,
		reqSeqNum:   make(chan int),
		killClient:  make(chan int),
		shutdown:    make(chan int),
	}

	// Start goroutines
	go s.masterHandler()
	go s.netHandler()

	return s, nil

}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	if s.toRead == nil {
		LOGE.Println("Connection has been closed")
		return 0, nil, errors.New("Connection has been closed")
	}

	msg := <-s.toRead.out

	if msg.Type == MsgAck {
		LOGE.Println("Connection has been closed")
		return 0, nil, errors.New("Connection has been closed")
	}
	return msg.ConnID, msg.Payload, nil
}

func (s *server) Write(connID int, payload []byte) error {

	s.reqSeqNum <- connID
	seqNum := <-s.reqSeqNum
	if seqNum == -1 {
		LOGE.Printf("Unknown connId %d", connID)
		return errors.New("Unknown connId")
	} else {
		msg := NewData(connID, seqNum, payload)
		LOGV.Println(fmt.Sprint("Write requested: ", msg.String()))
		s.send.in <- msg
		return nil
	}
}

func (s *server) CloseConn(connID int) error {
	s.killClient <- connID
	r := <-s.killClient
	if r == -1 {
		return errors.New("Unknown client")
	} else {
		return nil
	}
}

func (s *server) Close() error {
	// Finish it off
	s.shutdown <- 0
	// Block until finished
	<-s.shutdown
	return nil
}

// Central handler, deals with synchronous stuff
func (s *server) masterHandler() {
	tick := time.NewTicker(time.Duration(s.params.EpochMillis) * time.Millisecond)
	ticker := tick.C

	for {
		select {
		case <-ticker:
			// epoch event
			s.epochHandler()
		case clientMsg := <-s.receive:
			// new inbound message
			s.receiveHandler(clientMsg)
		case msg := <-s.send.out:
			// new outbound message

			// Get it's client
			client, ok := s.clients[msg.ConnID]
			if ok {

				if msg.Type == MsgData {
					LOGV.Printf("Checking if %d is >= %d and < %d", msg.SeqNum, client.sWindow.base, client.sWindow.base+client.sWindow.size)
					if client.sWindow.inWindow(msg) { // Should we queue it
						LOGV.Println(fmt.Sprint("Write adding message to window: ", string(msg.Payload)))
						client.sWindow.add(msg)
						s.sendMessage(msg)
					} else { // Or just send it
						LOGV.Println(fmt.Sprint("Write deferred, queueing: ", string(msg.Payload)))
						client.toWrite.PushBack(msg)
					}

				} else {
					s.sendMessage(msg)
				}
			}
		case connId := <-s.reqSeqNum:
			// Synchronously send new sequence number
			client, ok := s.clients[connId]
			if ok {
				client.seqNum++
				s.reqSeqNum <- client.seqNum
			} else {
				s.reqSeqNum <- -1
			}
		case connId := <-s.killClient:
			// Attempt to kill a client
			client, ok := s.clients[connId]
			if ok {
				// Remove client
				delete(s.clients, connId)    // From the client map
				delete(s.addrs, client.addr) // and from the address map
			} else {
				s.reqSeqNum <- -1
			}
		case <-s.shutdown:
			// No more reads will be called
			s.toRead.Close()
			// This should close the netHandler
			s.conn.Close()
			// Stop the ticker.
			tick.Stop()
			return
		}
	}
}

// Listens for inbound messages
func (s *server) netHandler() {
	var buf [BUFFSIZE]byte // create buffer
	for {
		select { // Will be listening on kill signals later
		default:
			// Wait for new inbound message
			n, addr, err := s.conn.ReadFromUDP(buf[0:])
			if err != nil {
				LOGE.Println(fmt.Sprint("Error reading: ", err))
				return
			} else {
				var msg Message
				json.Unmarshal(buf[0:n], &msg)
				LOGV.Println(fmt.Sprint("Read has occurred: ", msg.String()))

				s.receive <- &clientMessage{
					addr: addr,
					msg:  msg,
				}
			}
		}
	}
}

func (s *server) epochHandler() {
	// For every client we know about
	for connId, client := range s.clients {
		// Update active connection counters.
		if client.gotMail == true {
			client.gotMail = false
			client.timeoutCount = 0
		} else {
			client.timeoutCount++
			LOGV.Printf("Timeout count for client %d is %d", connId, client.timeoutCount)
			if client.timeoutCount == s.params.EpochLimit {
				LOGV.Printf("Timeout on client %d", connId)

				// Remove client
				delete(s.clients, connId)    // From the client map
				delete(s.addrs, client.addr) // and from the address map
			}
		}

		// Resend acknowledgements if window's range = 1
		if client.sWindow.base == 1 {
			s.send.in <- client.rWindow.window[0]
		}

		// Resend any non-ack'd data messages
		for tmp := client.sWindow.base; tmp < client.sWindow.base+client.sWindow.size; tmp++ {
			resend, ok := client.sWindow.window[tmp]
			// If it exists and it's not confirmed, resend it
			if ok && resend.Type == MsgData {
				LOGV.Println(fmt.Sprint("Resending data: ", resend.String()))
				s.send.in <- resend
			}
		}

		// Reack last w data messages
		for tmp := client.rWindow.base; tmp < client.rWindow.base+client.rWindow.size; tmp++ {
			reack, ok := client.rWindow.window[tmp]
			// There's a message in this bucket, reack it.
			if ok {
				reack_msg := NewAck(reack.ConnID, reack.SeqNum)
				LOGV.Println(fmt.Sprint("Resending ack: ", reack_msg.String()))
				s.send.in <- reack_msg
			}
		}

	}
}

func (s *server) receiveHandler(clientMsg *clientMessage) {
	switch clientMsg.msg.Type {
	case MsgConnect:
		// Do we have a connection for them yet? If so, nothing to do here
		_, ok := s.addrs[clientMsg.addr]
		if !ok {
			// Make new connection
			cl := &cli{
				addr:         clientMsg.addr,
				connId:       s.connIdCount,
				seqNum:       0,
				sWindow:      NewSendWindow(s.params.WindowSize),
				rWindow:      NewReceiveWindow(s.params.WindowSize),
				gotMail:      true,
				timeoutCount: 0,
				toWrite:      list.New(),
			}

			LOGV.Printf("Client %d connected", cl.connId)
			confirmation := NewAck(s.connIdCount, 0)

			// Add connection confirmation to receiveWindow so it gets sent out on epochs
			cl.rWindow.window[0] = confirmation

			// Update maps
			s.clients[cl.connId] = cl
			s.addrs[clientMsg.addr] = cl.connId

			// Send message, update connection counter
			s.send.in <- confirmation
			s.connIdCount++
		}
	case MsgData:
		// Are they still connected?
		client, ok := s.clients[clientMsg.msg.ConnID]
		if !ok {
			LOGE.Println(fmt.Sprint("Got message from untracked client: ", clientMsg.msg.String()))
			return
		}

		// We got mail!
		client.gotMail = true

		// Add message to that client's received window.
		client.rWindow.receive(&clientMsg.msg)
		s.send.in <- NewAck(clientMsg.msg.ConnID, clientMsg.msg.SeqNum)

		// Update read queue
		for {
			readMsg, ok := client.rWindow.readNext()
			if !ok { // Nothing new to read
				break
			} else { // Something new, put it in the toRead queue
				s.toRead.in <- readMsg
			}
		}
	case MsgAck:
		LOGV.Println(fmt.Sprint("Got acknowledgement ", clientMsg.msg.String()))

		// Get that client
		client, ok := s.clients[clientMsg.msg.ConnID]
		if !ok {
			LOGE.Println(fmt.Sprint("Got message from untracked client: ", clientMsg.msg.String()))
			return
		}

		// We got mail!
		client.gotMail = true

		// Check if it's an "I'm waiting on data" message
		if clientMsg.msg.SeqNum == 0 {
			return
		}
		// Acknowledge any unack'd messages
		client.sWindow.ack(&clientMsg.msg)

		// Update write queue
		for {
			next := client.toWrite.Front()
			// Is there a next? Are we even in window range?
			if next == nil || !client.sWindow.inWindow((next.Value).(*Message)) {
				break
			} else {
				client.sWindow.add((next.Value).(*Message))
				s.send.in <- (next.Value).(*Message)
				client.toWrite.Remove(next)
			}
		}
	default:
		LOGE.Println(fmt.Sprint("What even, how did you get here: ", clientMsg.msg.String()))
	}

}

func (s *server) sendMessage(msg *Message) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return errors.New("Error marshalling")
	}
	LOGV.Println(string(bytes))

	client, ok := s.clients[msg.ConnID]
	if !ok {
		LOGE.Printf("Attempted to send message to non-existant connID %d", msg.ConnID)
		return errors.New(fmt.Sprintf("Attempted to send message to non-existant connID %d", msg.ConnID))
	}

	_, err = s.conn.WriteToUDP(bytes, client.addr)
	if err != nil {
		LOGE.Printf("Error writing to connId %d", msg.ConnID)
		return errors.New("Error writing")
	}
	LOGV.Println(fmt.Sprint("Write occurred: ", msg.String()))

	return nil
}
