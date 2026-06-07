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

func (c *messageChannel) popFromReorderBuf() (*Message, bool) {
	for i, msg := range c.reorderBuf {
		if msg.seqNum == c.recvSeqNum {
			// Remove from buffer
			c.reorderBuf = append(c.reorderBuf[:i], c.reorderBuf[i+1:]...)
			c.recvSeqNum++
			return &msg.msg, true
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
