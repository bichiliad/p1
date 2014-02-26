package lsp

import (
	"container/list"
)

type T *Message

type UChannel struct {
	in      chan T // input
	out     chan T // output
	closing bool
}

func NewUnboundedChannel() *UChannel {
	in, out := make(chan T), make(chan T)

	u := &UChannel{
		in:      in,
		out:     out,
		closing: false,
	}

	go u.handleBuffer(in, out)

	return u
}

func (u *UChannel) Close() {
	u.closing = true
	u.CloseIn()
}

func (u *UChannel) CloseIn() {
	close(u.in)
	u.in = nil
}

// From Piazza
func (u *UChannel) handleBuffer(in <-chan T, out chan<- T) {
	defer LOGE.Println("Shutdown - unbounded channel should be closing now")
	defer close(out)

	// This list will store all values received from 'in'.
	// All values should eventually be sent back through 'out',
	// even if the 'in' channel is suddenly closed.
	buffer := list.New()

	for {
		// Make sure that the list always has values before
		// we select over the two channels.
		if buffer.Len() == 0 {
			v, ok := <-in
			if !ok {
				// 'in' has been closed. Flush all values
				// in the buffer and return.
				u.flush(buffer, out)
				return
			}
			buffer.PushBack(v)
		}

		select {
		case v, ok := <-in:
			if !ok {
				// 'in' has been closed. Flush all values
				// in the buffer and return.
				u.flush(buffer, out)
				return
			}
			buffer.PushBack(v)
		case out <- (buffer.Front().Value).(T):
			buffer.Remove(buffer.Front())
		}
	}
}

// Blocks until all values in the buffer have been sent through
// the 'out' channel.
func (u *UChannel) flush(buffer *list.List, out chan<- T) {
	if out == nil || u.closing { // Out has been closed, forget it.
		return
	}
	for e := buffer.Front(); e != nil; e = e.Next() {
		out <- (e.Value).(T)
	}
	// Signify that the channel has closed.

	out <- (NewAck(-1, -1))
}
