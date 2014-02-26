// Contains the implementation of a LSP server.

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

// var LOGS = log.New(os.Stdout, "  [VERBOSE] ", log.Lmicroseconds|log.Lshortfile)

var LOGS = log.New(ioutil.Discard, "  [VERBOSE] ", log.Lmicroseconds|log.Lshortfile)

const (
	BUFFSIZE = 1024
)

type server struct {
	clients          map[int]*cli        // Map of all clients
	addrs            map[string]int      // Maps addresses to connId's
	conn             *lspnet.UDPConn     // UDP connection to listen on
	send             *UChannel           // Channel to send outbound messages on
	receive          chan *clientMessage // Channel for inbound messages
	params           *Params             // Configurable parameters
	toRead           *UChannel           // Things waiting to be read
	connIdCount      int                 // What's our next connId?
	reqSeqNum        chan int            // Request a new seqNumber atomically
	resSeqNum        chan int            // Send a new seqNumber atomically
	killClient       chan int            // For closing clients
	killClientResult chan int            // For response to client close requests
	shutdown         chan int            // Request a shutdown
	shutdownComplete chan int            // Lets Close() know we're done.
	kill             chan int            // for closing the net handler
	closing          bool                // Closing state.
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
		clients:          make(map[int]*cli),
		addrs:            make(map[string]int),
		conn:             conn,
		send:             NewUnboundedChannel(),
		receive:          make(chan *clientMessage, channelBufferSize),
		params:           params,
		toRead:           NewUnboundedChannel(),
		connIdCount:      1,
		reqSeqNum:        make(chan int),
		resSeqNum:        make(chan int),
		kill:             make(chan int),
		killClient:       make(chan int),
		killClientResult: make(chan int),
		shutdown:         make(chan int),
		shutdownComplete: make(chan int),
		closing:          false,
	}

	// Start goroutines
	go s.masterHandler()
	go s.netHandler()

	LOGS.Println("Server started listening")

	return s, nil

}

func (s *server) Read() (int, []byte, error) {
	if s.toRead == nil {
		LOGE.Println("Connection has been closed")
		return 0, nil, errors.New("Connection has been closed")
	}

	msg := <-s.toRead.out

	if msg.Type == MsgAck {
		LOGE.Println("Connection has been closed")
		return 0, nil, errors.New("Connection has been closed")
	} else if msg.Type == MsgConnect {
		LOGE.Println("A client has disconnected")
		return 0, nil, errors.New("A client has disconnected")
	}
	return msg.ConnID, msg.Payload, nil
}

func (s *server) Write(connID int, payload []byte) error {

	s.reqSeqNum <- connID
	seqNum := <-s.resSeqNum
	if seqNum == -1 {
		LOGE.Printf("Unknown connId %d", connID)
		return errors.New("Unknown connId")
	} else {
		msg := NewData(connID, seqNum, payload)
		LOGS.Println(fmt.Sprint("Write requested: ", msg.String()))
		s.send.in <- msg
		return nil
	}
}

func (s *server) CloseConn(connID int) error {
	LOGV.Printf("CloseConn - called with id %d", connID)
	s.killClient <- connID
	r := <-s.killClientResult
	if r == -1 {
		LOGV.Printf("CloseConn - Client %d does not exist", connID)
		return errors.New("Unknown client")
	} else {
		LOGV.Printf("CloseConn - Successfully killed client %d", connID)
		return nil
	}
}

func (s *server) Close() error {
	defer LOGS.Println("Server Shutdown - Close() return")

	LOGS.Println("Server Shutdown - Close() called")
	// Finish it off
	s.shutdown <- 0
	LOGS.Println("Server Shutdown - Close() blocking")

	// Block until finished
	<-s.shutdownComplete
	return nil
}

func (s *server) doneSending() bool {
	// Check to see if each client isn't waiting on any acks, and doesn't have queued writes
	for _, client := range s.clients {
		if client.sWindow.yetToSend() != 0 || client.toWrite.Len() != 0 {
			LOGS.Printf("Shutdown - Can't close yet, client %d still has (%d, %d) messages", client.connId,
				client.sWindow.yetToSend(), client.toWrite.Len())
			return false
		}
	}

	return true
}

// Central handler, deals with synchronous stuff
func (s *server) masterHandler() {
	defer LOGS.Println("Server Shutdown - master Returned")
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
			if s.closing && s.doneSending() {
				LOGS.Println("Server Shutdown - Starting master shutdown")
				// This should close the netHandler
				// s.conn.Close()
				close(s.kill)
				// Stop the ticker.
				tick.Stop()
				// Close listener
				s.conn.Close()
				close(s.shutdownComplete)
				return

			}
		case msgElem := <-s.send.out:
			// new outbound message
			msg := (*Message)(msgElem)
			// Get it's client
			client, ok := s.clients[msg.ConnID]
			if ok {

				if msg.Type == MsgData {
					LOGS.Printf("Checking if %d is >= %d and < %d", msg.SeqNum, client.sWindow.base, client.sWindow.base+client.sWindow.size)
					if client.sWindow.inWindow(msg) { // Should we send it
						LOGS.Println(fmt.Sprint("Write adding message to window: ", string(msg.Payload)))
						client.sWindow.add(msg)
						s.sendMessage(msg, client.addr)
					} else { // Or just queue it
						LOGS.Println(fmt.Sprint("Write deferred, queueing: ", string(msg.Payload)))
						client.toWrite.PushBack(msg)
					}

				} else {
					s.sendMessage(msg, client.addr)
				}
			}
		case connId := <-s.reqSeqNum:
			// Synchronously send new sequence number
			client, ok := s.clients[connId]
			if ok {
				client.seqNum++
				s.resSeqNum <- client.seqNum
			} else {
				s.resSeqNum <- -1
			}
		case connId := <-s.killClient:
			// Attempt to kill a client
			client, ok := s.clients[connId]
			if ok {
				// Remove client
				delete(s.clients, connId)             // From the client map
				delete(s.addrs, client.addr.String()) // and from the address map
				s.killClientResult <- 0
			} else {
				s.killClientResult <- -1
			}
		case <-s.shutdown:
			// Set state to closing
			s.closing = true
			// No more reads will be called
			s.toRead.CloseIn()
			for _ = range s.toRead.out {
				// Toss out unread messages
			}
			// Business as usual for now.
		}
	}
}

// Listens for inbound messages
func (s *server) netHandler() {
	defer LOGS.Println("Server shutdown - netHandler return")
	var buf [BUFFSIZE]byte // create buffer
	for {
		select {
		case <-s.kill:
			return
		default:
			// Wait for new inbound message
			n, addr, err := s.conn.ReadFromUDP(buf[0:])
			if err != nil {
				LOGE.Println(fmt.Sprint("Error reading: ", err))
				return
			} else {
				var msg Message
				json.Unmarshal(buf[0:n], &msg)
				LOGS.Println(fmt.Sprint("Read has occurred: ", msg.String()))

				s.receive <- &clientMessage{
					addr: addr,
					msg:  msg,
				}
			}
		}
	}
}

func (s *server) epochHandler() {
	LOGS.Printf("Server epoch")

	// For every client we know about
	for connId, client := range s.clients {
		// Update active connection counters.
		if client.gotMail == true {
			client.gotMail = false
			client.timeoutCount = 0
		} else {
			client.timeoutCount++
			LOGS.Printf("Timeout count for client %d is %d", connId, client.timeoutCount)
			if client.timeoutCount == s.params.EpochLimit {
				LOGS.Printf("Timeout on client %d", connId)

				// Remove client
				delete(s.clients, connId)             // From the client map
				delete(s.addrs, client.addr.String()) // and from the address map
				s.toRead.in <- NewConnect()
				break
			}
		}

		// Resend acknowledgements if window's range = 1
		if client.sWindow.base == 1 {
			s.send.in <- client.rWindow.window[0]
		}

		// Reack last w data messages
		for tmp := client.rWindow.base; tmp < client.rWindow.base+client.rWindow.size; tmp++ {
			reack, ok := client.rWindow.window[tmp]
			// There's a message in this bucket, reack it.
			if ok {
				reack_msg := NewAck(reack.ConnID, reack.SeqNum)
				LOGS.Println(fmt.Sprint("Resending ack: ", reack_msg.String()))
				// s.send.in <- reack_msg
				s.sendMessage(reack_msg, client.addr)
			}
		}

		// Resend any non-ack'd data messages
		for tmp := client.sWindow.base; tmp < client.sWindow.base+client.sWindow.size; tmp++ {
			resend, ok := client.sWindow.window[tmp]
			// If it exists and it's not confirmed, resend it
			if ok && resend.Type == MsgData {
				LOGS.Println(fmt.Sprint("Resending data: ", resend.String()))
				// s.send.in <- resend
				s.sendMessage(resend, client.addr)
			}
		}

	}
}

func (s *server) receiveHandler(clientMsg *clientMessage) {
	switch clientMsg.msg.Type {
	case MsgConnect:
		// Do we have a connection for them yet? If so, nothing to do here
		_, ok := s.addrs[clientMsg.addr.String()]
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

			confirmation := NewAck(s.connIdCount, 0)

			// Add connection confirmation to receiveWindow so it gets sent out on epochs
			cl.rWindow.window[0] = confirmation

			// Update maps
			s.clients[cl.connId] = cl
			s.addrs[clientMsg.addr.String()] = cl.connId

			// Send message, update connection counter
			s.send.in <- confirmation
			s.connIdCount++
		}
	case MsgData:
		// Are they still connected?
		client, ok := s.clients[clientMsg.msg.ConnID]
		if !ok {
			LOGS.Println(fmt.Sprint("Got message from untracked client: ", clientMsg.msg.String()))
			return
		}

		// We got mail!
		client.gotMail = true

		// Add message to that client's received window.
		client.rWindow.receive(&clientMsg.msg)
		// s.send.in <- NewAck(clientMsg.msg.ConnID, clientMsg.msg.SeqNum)
		s.sendMessage(NewAck(clientMsg.msg.ConnID, clientMsg.msg.SeqNum), clientMsg.addr)

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
		LOGS.Println(fmt.Sprint("Got acknowledgement ", clientMsg.msg.String()))

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

func (s *server) sendMessage(msg *Message, addr *lspnet.UDPAddr) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return errors.New("Error marshalling")
	}

	// client, ok := s.clients[msg.ConnID]
	// if !ok {
	// 	LOGE.Printf("Attempted to send message to non-existant connID %d", msg.ConnID)
	// 	return errors.New(fmt.Sprintf("Attempted to send message to non-existant connID %d", msg.ConnID))
	// }

	_, err = s.conn.WriteToUDP(bytes, addr)
	if err != nil {
		LOGE.Printf("Error writing to connId %d", msg.ConnID)
		return errors.New("Error writing")
	}
	// LOGS.Println(fmt.Sprint("Server write occurred: ", msg.String()))

	return nil
}
