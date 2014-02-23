package lsp

import (
	"container/list"
)

type T *Message

type UChannel struct {
	in  chan T // input
	out chan T // output
}

func NewUnboundedChannel() *UChannel {
	in, out := make(chan T), make(chan T)
	go handleBuffer(in, out)

	return &UChannel{
		in:  in,
		out: out,
	}
}

func (u *UChannel) CloseIn() {
	close(u.in)
}

func (u *UChannel) CloseOut() {
	close(u.out)
}

// From Piazza
func handleBuffer(in <-chan T, out chan<- T) {
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
				flush(buffer, out)
				return
			}
			buffer.PushBack(v)
		}

		select {
		case v, ok := <-in:
			if !ok {
				// 'in' has been closed. Flush all values
				// in the buffer and return.
				flush(buffer, out)
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
func flush(buffer *list.List, out chan<- T) {
	for e := buffer.Front(); e != nil; e = e.Next() {
		out <- (e.Value).(T)
	}
	// Signify that the channel has closed.
	out <- (NewAck(-1, -1))
}
