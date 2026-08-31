package objproto

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
)

type Message struct {
	From         ConnectionID
	PacketNumber PacketNumber
	Data         []byte
}

type internalMessage struct {
	msg    Message
	seqNum uint64
}

// multi-writer single-reader message channel
type messageChannel struct {
	messageChan chan internalMessage
	ctx         context.Context
	cancel      context.CancelCauseFunc
	cancelLock  sync.RWMutex
	closed      sync.Once
	senderWg    sync.WaitGroup
	logger      *slog.Logger
	reorderBuf  []internalMessage
	recvSeqNum  uint64
	seqNum      atomic.Uint64
}

func NewMessageChannel(buffer int, logger *slog.Logger) *messageChannel {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &messageChannel{
		messageChan: make(chan internalMessage, buffer),
		ctx:         ctx,
		cancel:      cancel,
		logger:      logger,
	}
}

func (c *messageChannel) Logger() *slog.Logger {
	return c.logger
}

func (c *messageChannel) CloseChannel() {
	c.closed.Do(func() {
		c.cancelLock.Lock()
		c.cancel(ErrChannelClosed) // Cancel the context to stop the goroutine
		c.cancelLock.Unlock()
		c.senderWg.Wait()
		close(c.messageChan) // Close the message channel after all senders are done
	})
}

var ErrChannelClosed = errors.New("message channel closed")

// popFromReorderBuf takes the next message in sequence out of the reorder
// buffer, if it is there.
//
// Indexed, not `for i, msg := range`. Returning `&msg.msg` from a range loop
// makes the loop variable escape, so the copy is heap-allocated on EVERY
// iteration and all but the matching one is thrown away immediately. That was
// 44% of all allocations on a bulk transfer -- about 94 per received packet,
// of which one was the message actually delivered -- because the buffer runs
// deep: a message that had to take SendMessage's goroutine path holds up
// every message behind it, and they pile up here waiting for it.
//
// The scan and the removal are still O(n) each, so this stays O(n^2) over a
// deep buffer. Only the allocation is fixed here; measure before assuming the
// scan itself is worth restructuring.
func (c *messageChannel) popFromReorderBuf() (*Message, bool) {
	for i := range c.reorderBuf {
		if c.reorderBuf[i].seqNum == c.recvSeqNum {
			msg := c.reorderBuf[i].msg // the one copy that is kept
			// Remove from buffer
			c.reorderBuf = append(c.reorderBuf[:i], c.reorderBuf[i+1:]...)
			c.recvSeqNum++
			return &msg, true
		}
	}
	return nil, false
}

func (c *messageChannel) ReceiveMessage() (*Message, error) {
	if msg, ok := c.popFromReorderBuf(); ok {
		return msg, nil
	}
	for msg := range c.messageChan {
		if msg.seqNum == c.recvSeqNum {
			c.recvSeqNum++
			return &msg.msg, nil
		} else {
			// Out of order, store in buffer
			c.reorderBuf = append(c.reorderBuf, msg)
		}
	}
	return nil, ErrChannelClosed // Return error if the channel is closed
}

var ErrTimeout = errors.New("message receive timeout")

func (c *messageChannel) ReceiveMessageContext(ctx context.Context) (*Message, error) {
	for {
		if msg, ok := c.popFromReorderBuf(); ok {
			return msg, nil
		}
		select {
		case msg, ok := <-c.messageChan:
			if !ok {
				return nil, ErrChannelClosed
			}
			if msg.seqNum == c.recvSeqNum {
				c.recvSeqNum++
				return &msg.msg, nil
			} else {
				// Out of order, store in buffer
				c.reorderBuf = append(c.reorderBuf, msg)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		}
	}
}

// SendMessage hands one received message to the reader without ever blocking
// its caller. That "never blocks" is a hard requirement, not a preference:
// the only caller is receiveApplication, which runs on the socket-reading
// goroutine while holding both endpointLock.RLock and activeConn.mu — so a
// blocked send here stalls every other connection on the endpoint.
//
// The channel is small, so a burst can fill it, and the fallback for that is
// a goroutine per message. Trying the send first means that goroutine is paid
// only when the reader is actually behind, instead of on every packet:
// nothing about the caller's locks changes, and the burst case still cannot
// block. Out-of-order arrival between the two paths is already handled — the
// seqNum stamped below is what ReceiveMessage reorders on, and it is taken
// before either path runs.
func (c *messageChannel) SendMessage(msg Message) error {
	c.cancelLock.RLock()
	select {
	case <-c.ctx.Done():
		c.cancelLock.RUnlock()
		return c.ctx.Err()
	default:
	}
	c.senderWg.Add(1)
	c.cancelLock.RUnlock()
	seqNum := c.seqNum.Add(1) - 1
	select {
	case c.messageChan <- internalMessage{msg: msg, seqNum: seqNum}:
		c.senderWg.Done()
		return nil
	default:
	}
	go func() {
		defer c.senderWg.Done()
		select {
		case c.messageChan <- internalMessage{msg: msg, seqNum: seqNum}:
		case <-c.ctx.Done():
			return
		}
	}()
	return nil
}

func (c *messageChannel) SendMessageBlocking(msg Message) error {
	c.cancelLock.RLock()
	select {
	case <-c.ctx.Done():
		c.cancelLock.RUnlock()
		return c.ctx.Err()
	default:
	}
	c.senderWg.Add(1)
	c.cancelLock.RUnlock()
	defer c.senderWg.Done()
	seqNum := c.seqNum.Add(1) - 1
	select {
	case c.messageChan <- internalMessage{msg: msg, seqNum: seqNum}:
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
	return nil
}
