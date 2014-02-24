package lsp

import (
	"errors"
	"fmt"
)

type SendWindow struct {
	window map[int]*Message // Maps id to message status
	base   int              // Bottom of window
	size   int              // Size of window
}

type ReceiveWindow struct {
	window   map[int]*Message // Maps id to message status
	base     int              // Bottom of window
	size     int              // Size of window
	readHead int              // Where reads occur
}

func NewSendWindow(windowSize int) *SendWindow {
	return &SendWindow{
		window: make(map[int]*Message),
		base:   1,
		size:   windowSize,
	}
}

func NewReceiveWindow(windowSize int) *ReceiveWindow {
	return &ReceiveWindow{
		window:   make(map[int]*Message),
		base:     1,
		size:     windowSize,
		readHead: 1,
	}
}

// Is the given message inside the current window range?
// Used to determine if something should be sent, or queued.
// Does not check if value is stored or not.
func (s *SendWindow) inWindow(msg *Message) bool {
	// Should be at least base, and at most base + size - 1
	return msg.SeqNum >= s.base && msg.SeqNum < s.base+s.size
}

// Returns the number of values in the current window that are still
// waiting for acknowledgements
func (s *SendWindow) yetToSend() int {
	count := 0
	for tmp := s.base; tmp < s.base+s.size; tmp++ {
		msg, ok := s.window[tmp]
		if ok && msg.Type == MsgData {
			count++
		}
	}
	return count
}

func (s *SendWindow) add(msg *Message) error {
	if !s.inWindow(msg) {
		return errors.New(fmt.Sprint("Message not in window.", msg.String()))
	}

	_, ok := s.window[msg.SeqNum]

	if !ok { // Message not sent yet, this is normal.
		s.window[msg.SeqNum] = msg
	} // Saving else for if I need some more verbose logging

	return nil

}

// Takes in a received acknowledged message
func (s *SendWindow) ack(msg *Message) error {
	if msg.Type != MsgAck { // Only ack with an ack message. Sanity check.
		return errors.New("Cannot ack with a non-ack message")
	}

	sentMsg, ok := s.window[msg.SeqNum]

	if !ok { // We're acking a message we never sent. That's not ok
		LOGE.Println(fmt.Sprint("Acking a nil window entry: ", msg.String()))
		return errors.New(fmt.Sprint("Got acknowledgement for unsent msg %d", msg.SeqNum))
	} else if sentMsg.Type == MsgAck { // We're acking an ack, nothing to do
		return nil
	} else if sentMsg.Type == MsgData { // New unconfirmed message, update with ack
		s.window[msg.SeqNum] = msg
		// Update window
		for { // Look for next unconfirmed message, or nil.
			if temp, ok := s.window[s.base]; !ok || temp.Type == MsgData {
				break // new base!
			}
			s.base++ // awh no new base yet. Try again!
		}
		// Base updated by this point
		return nil
	} else { // Should never hit this.
		return errors.New(fmt.Sprint("Unhandled error when acking:", msg.String()))
	}

}

// We've received a message, save it and update the window if necessary.
func (r *ReceiveWindow) receive(msg *Message) {
	if msg.Type != MsgData {
		LOGE.Println(fmt.Sprint("Tried to add non-data to receive window", msg.String()))
	}
	r.window[msg.SeqNum] = msg
	// Do we need to update our window?
	if msg.SeqNum >= r.base+r.size {
		r.base = msg.SeqNum - r.size + 1
	}
}

// Reads any available messages, or returns (nil, false)
func (r *ReceiveWindow) readNext() (*Message, bool) {
	msg, ok := r.window[r.readHead]
	if !ok { // that spot is nil, don't do anything
		return nil, false
	} else {
		r.readHead++ // Update the read head for next call
		return msg, true
	}
}
