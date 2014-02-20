package lsp

import "fmt"
import "container/list"

// SendWindow: Window for outgoing messages

type SendWindow struct {
	messages *list.List // The messages the window contains
	size     int        // How big the window is
	base     int        // The lowest value in the window
}

// Constructor
func NewSendWindow(size int) *SendWindow {
	return &SendWindow{
		messages: list.New(),
		size:     size,
		base:     0,
	}
}

// How many things in the window right now?
func (w *SendWindow) cap() int {
	return w.messages.Len()
}

// Returns true if message is in range of window, false otherwise
func (w *SendWindow) inRange(msg Message) bool {
	return msg.SeqNum >= w.base && msg.SeqNum < (w.base+w.size)
}

// Add: Adds to window, queues otherwise.
// Returns true if added to window, false if out of window range
func (w *SendWindow) add(msg Message) bool {
	if !w.inRange(msg) { // Not in range? Toss it!
		fmt.Println("Why are you trying to add that to the window?")
		return false
	} else {
		insertSorted(w.messages, msg)
		return true
	}
}

// Remove: removes from window if present, ignores otherwise.
// Also updates window range
func (w *SendWindow) remove(msg Message) {

}

// Inserts a message into a list.
// Assumes the list, if it's a window, can fit it.
func insertSorted(list *list.List, msg Message) {
	// Is the list empty?
	if list.Front() == nil {
		_ = list.PushFront(msg)
		return
	}

	// Find the right spot.
	for e := list.Front(); e != nil; e.Next() {
		cur := e.Value.(Message)

		// Is our message in between two list members, or a list member and the end?
		if cur.SeqNum < msg.SeqNum &&
			(e.Next() == nil || msg.SeqNum < e.Next().Value.(Message).SeqNum) {

			list.InsertAfter(msg, e)
			return
		}

	}
}
