package trsf

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/trsf/congestion"
	"github.com/on-keyday/objtrsf/trsf/mtu"
	"github.com/on-keyday/objtrsf/trsf/wire"
)

type IDIssuer struct {
	m      sync.Mutex
	nextID StreamID
}

func NewIDIssuer(start StreamID) *IDIssuer {
	return &IDIssuer{
		nextID: start,
	}
}

func (i *IDIssuer) Next() StreamID {
	i.m.Lock()
	defer i.m.Unlock()
	id := i.nextID
	i.nextID = i.nextID.Next()
	return id
}

var _ Multiplexer = (*Streams)(nil)

type SendAction struct {
	base         *Streams
	fired        bool
	PacketNumber objproto.PacketNumber
	//Packet       *wire.StreamPacket
	//Window       *wire.UpdateWindow
	//Cancel       *wire.CancelStreamPacket
	//ACK          *wire.StreamACKPacket
	Data []byte
	ACK  []byte
}

type UnderlayingSendTransport interface {
	SendMessage(msg []byte) (int, objproto.PacketNumber, error)
	SendMessageWithPacketNumber(msg []byte, pn objproto.PacketNumber) (int, objproto.PacketNumber, error)
}

type PacketNumberIssuer interface {
	ConsumePacketNumber() objproto.PacketNumber
}

const TrsfPacket = "p"

func (s *SendAction) Send(ctx context.Context, conn UnderlayingSendTransport) error {
	if s.fired {
		return nil
	}
	s.fired = true
	if s.ACK != nil {
		_, _, err := conn.SendMessage(s.ACK)
		if err != nil {
			return err
		}
	}
	if s.Data != nil {
		_, _, err := conn.SendMessageWithPacketNumber(s.Data, s.PacketNumber)
		if err != nil {
			return err
		}
	}
	return nil
}

type Streams struct {
	ctx         context.Context
	streamsLock sync.Mutex
	sendStreams map[StreamID]*sendStream
	recvStreams map[StreamID]*recvStream

	bidiIDIssuer *IDIssuer
	uniIDIssuer  *IDIssuer

	sendTrigger   *withTriggerQueue[sendStream]
	updateWindow  *withTriggerQueue[recvStream]
	cancelTrigger *withTriggerQueue[recvStream]

	sh *SentPacketHandler

	pt *PacketNumTracker

	recv *withTriggerQueue[objproto.Message]
	send *withTriggerQueue[SendAction]

	isServer bool

	logger *slog.Logger

	newRecvStreamQueue chan ReceiveStream
	newBidiStreamQueue chan BidirectionalStream

	pnIssuer PacketNumberIssuer

	mtu *mtu.MTUTracker

	// loopIter counts run-loop iterations. Exposed via GetInternalState so a
	// test (and production diagnostics) can detect a busy-spin: a wedged loop
	// that wakes immediately every iteration without making progress inflates
	// this counter far faster than a healthy loop driven by I/O notifications.
	loopIter atomic.Uint64
}

type InternalSentPacket struct {
	Kind       wire.ApplicationPayloadKind
	StreamID   StreamID
	SentTime   time.Time
	PacketSize int
	IsMTUProbe bool
}

type InternalState struct {
	ActiveSendStreams    int
	ActiveReceiveStreams int
	CurrentMTU           int
	SendQueueLength      int
	ReceiveQueueLength   int
	SendActionCount      int
	UpdateWindowCount    int
	CancelStreamCount    int
	BytesInFlight        int
	CongestionWindow     int
	SmoothedRTT          time.Duration
	RTTVariance          time.Duration
	SentPackets          []InternalSentPacket
	LoopIterations       uint64
}

func (s *Streams) GetInternalState() *InternalState {
	s.streamsLock.Lock()
	activeSendStream := len(s.sendStreams)
	activeRecvStream := len(s.recvStreams)
	s.streamsLock.Unlock()
	sentRanges, bytesInFlight, congestionWindow, smoothedRTT, rttVariance := s.sh.GetInternal()
	return &InternalState{
		ActiveSendStreams:    activeSendStream,
		ActiveReceiveStreams: activeRecvStream,
		CurrentMTU:           s.mtu.CurrentMTU(),
		ReceiveQueueLength:   s.recv.Len(),
		SendQueueLength:      s.send.Len(),
		SendActionCount:      s.sendTrigger.Len(),
		UpdateWindowCount:    s.updateWindow.Len(),
		CancelStreamCount:    s.cancelTrigger.Len(),
		BytesInFlight:        bytesInFlight,
		CongestionWindow:     congestionWindow,
		SmoothedRTT:          smoothedRTT,
		RTTVariance:          rttVariance,
		SentPackets:          sentRanges,
		LoopIterations:       s.loopIter.Load(),
	}
}

func (s *Streams) GetSendStream(id StreamID) SendStream {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	if sd, ok := s.sendStreams[id]; ok {
		return sd
	}
	return nil
}

func (s *Streams) GetReceiveStream(id StreamID) ReceiveStream {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	if rs, ok := s.recvStreams[id]; ok {
		return &wrapRecvStream{rs}
	}
	return nil
}

func (s *Streams) GetBidirectionalStream(id StreamID) BidirectionalStream {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	if rs, ok := s.recvStreams[id]; ok {
		if sd, ok := s.sendStreams[id]; ok {
			return &bidiStream{
				recvStream: rs,
				sendStream: sd,
			}
		}
	}
	return nil
}

func (s *Streams) getRecvStream(streamID StreamID) *recvStream {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	rs, ok := s.recvStreams[streamID]
	if !ok {
		if !s.isServer && streamID.IsServerInitiated() ||
			s.isServer && streamID.IsClientInitiated() {
			// new stream
			rs = newReceiveStream(s.ctx, s.logger, streamID, InitialFlowWindow, s.updateWindow, s.cancelTrigger)
			s.recvStreams[streamID] = rs
			// Notify the Accept* API of the new peer-initiated stream, but
			// NON-BLOCKING. Throughout this codebase streams are addressed by
			// ID (Get{Bidirectional,Receive}Stream / WaitForBidirectionalStream),
			// and nothing drains these accept queues — so a blocking send here
			// would stall getRecvStream, and the streamsLock it holds, the
			// moment the queue fills (~100 peer-initiated streams; e.g. a WebUI
			// snapshot poll opens a server send-stream every 5s). That wedged
			// the entire streams layer: all inbound demux and stream creation
			// froze while connection-level pings kept flowing. Drop the
			// notification rather than block the demux.
			if streamID.IsBidirectional() {
				sd := newSendStream(s.ctx, s.mtu, streamID, newFlowController(InitialFlowWindow), s.logger, s.sendTrigger)
				s.sendStreams[streamID] = sd
				bs := &bidiStream{
					recvStream: rs,
					sendStream: sd,
				}
				select {
				case s.newBidiStreamQueue <- bs:
				default:
				}
			} else {
				select {
				case s.newRecvStreamQueue <- &wrapRecvStream{rs}:
				default:
				}
			}
		}
	}
	return rs
}

func (s *Streams) removeRecvStream(streamID StreamID) {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	delete(s.recvStreams, streamID)
}

func (s *Streams) removeSendStream(streamID StreamID) {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	delete(s.sendStreams, streamID)
}

func (s *Streams) handlePacket(recvData *objproto.Message) {
	pkt := wire.StreamAppPacket{}
	err := pkt.DecodeExact(recvData.Data)
	if err != nil {
		s.logger.Error("failed to decode packet", "error", err)
		return
	}
	if data := pkt.StreamData(); data != nil {
		s.pt.InsertUnacked(uint64(recvData.PacketNumber))
		if data.IsProbe() {
			// MTU probe packet, no stream handling
			return
		}
		streamID := StreamID(0)
		if id := data.Id(); id != nil {
			streamID = StreamID(id.Value())
		}
		rs := s.getRecvStream(streamID)
		if rs == nil {
			s.logger.Error("received data for unknown stream", "stream_id", streamID)
			return
		}
		offset := uint64(0)
		if off := data.Offset(); off != nil {
			offset = off.Value()
		}
		_, err := rs.ProcessChunk(offset, data.Data, data.IsEof())
		if err != nil {
			s.logger.Error("failed to process received stream chunk", "stream_id", streamID, "error", err)
		}
		if rs.EOF() { // schedule removal
			time.AfterFunc(1*time.Minute, func() {
				s.removeRecvStream(streamID)
			})
		}
	} else if ack := pkt.StreamAck(); ack != nil {
		ranges, err := ParseTransferACK(ack)
		if err != nil {
			s.logger.Error("failed to parse transfer ack", "error", err)
			return
		}
		err = s.sh.ReceiveACK(time.Now(), ranges)
		if err != nil {
			s.logger.Error("failed to handle received ack", "error", err)
		}
	} else if uw := pkt.WindowUpdate(); uw != nil {
		s.pt.InsertUnacked(uint64(recvData.PacketNumber))
		streamID := StreamID(uw.Id.Value())
		s.streamsLock.Lock()
		rs, ok := s.sendStreams[streamID]
		s.streamsLock.Unlock()
		if !ok {
			s.logger.Error("received update window for unknown stream", "stream_id", streamID)
			return
		}
		rs.updateFlowWindow(int(uw.WindowMax.Value()))
	} else if cs := pkt.StreamCancel(); cs != nil {
		s.pt.InsertUnacked(uint64(recvData.PacketNumber))
		streamID := StreamID(cs.Id.Value())
		s.streamsLock.Lock()
		rs, ok := s.sendStreams[streamID]
		s.streamsLock.Unlock()
		if !ok {
			s.logger.Error("received cancel for unknown stream", "stream_id", streamID)
			return
		}
		rs.onCancel()
	} else {
		s.logger.Error("unknown stream packet type received", "type", pkt.Header.Kind)
	}
}

// nextWakeDeadline computes the wall-clock time the run loop should wake to do
// timer-driven work (loss detection / pacing), independent of the I/O
// notification channels it also selects on. A zero time means "no timer; block
// until a notification arrives".
func (s *Streams) nextWakeDeadline() time.Time {
	deadline := s.sh.LossDetectionTimeout()
	// Pacing governs only data sends, so fold the pacing timer into the wake
	// deadline ONLY when a send could actually happen on wake: congestion
	// control permits it (CanSend) AND there is stream data queued (sendTrigger
	// non-empty). Otherwise honoring it busy-spins the loop: pacer.Timer keys
	// off budgetAtLastSent and only advances on a send, so once the budget is
	// drained it returns a fixed PAST timestamp. Waking on that past time
	// without being able to send leaves lastSentTime unchanged, so the next
	// iteration sees the same past time and wakes immediately again — a 0-delay
	// spin. On the single-threaded wasm runtime that spin starves the JS event
	// loop, so inbound ACKs are never delivered and the in-flight packet is
	// never acked: the upload wedges until an idle timeout (observed as a WebUI
	// freeze at the final read-ack). When we cannot send, wait on the loss
	// timer plus the I/O notifications the caller also selects on; an ACK
	// (cwnd growth) or a new AppendData wakes the loop and re-evaluates.
	if s.sh.CanSend() && s.sendTrigger.Len() > 0 {
		pacer := s.sh.PacingTimeout()
		if !pacer.IsZero() && (deadline.IsZero() || pacer.Before(deadline)) {
			deadline = pacer
		}
	}
	return deadline
}

func (s *Streams) run(ctx context.Context) {
	for {
		s.loopIter.Add(1)
		recvedData := s.recv.Pop()
		if recvedData != nil {
			s.handlePacket(recvedData)
			continue
		}
		deadline := s.nextWakeDeadline()
		if deadline.IsZero() {
			select {
			case <-ctx.Done():
				return // end
			case <-s.sendTrigger.Notification(): // when new data to send
			case <-s.updateWindow.Notification(): // when new recv window to update
			case <-s.cancelTrigger.Notification(): // when stream cancel is requested
			case <-s.pt.NotifyReceive(): // when new stream ack to process
			case <-s.recv.Notification(): // when new data received
				continue
			case <-s.sh.Notification(): // when timer resets
				continue
			}
		} else {
			timer := time.Until(deadline)
			select {
			case <-ctx.Done():
				return // end
			case <-time.After(timer):
			case <-s.sendTrigger.Notification(): // when new data to send
			case <-s.updateWindow.Notification(): // when new recv window to update
			case <-s.cancelTrigger.Notification(): // when stream cancel is requested
			case <-s.pt.NotifyReceive(): // when new stream ack to process
			case <-s.recv.Notification(): // when new data received
				continue // process received data immediately
			case <-s.sh.Notification(): // when timer resets
				continue
			}
		}
		now := time.Now()
		lossTimeout := s.sh.LossDetectionTimeout()
		isPTO := false
		if !lossTimeout.IsZero() && now.After(lossTimeout) {
			var err error
			isPTO, err = s.sh.OnTimeout(now)
			if err != nil {
				s.logger.Error("error in loss detection timeout handling", "error", err)
			}
			if isPTO {
				s.logger.Debug("PTO fired")
			} else {
				s.logger.Debug("loss detection fired")
			}
		}
		ackRanges := s.pt.GenerateACK()
		var ack []byte
		if len(ackRanges) > 0 {
			ackPkt, err := TransferACK(ackRanges)
			if err != nil {
				s.logger.Error("failed to create transfer ack", "error", err)
			} else {
				encodedAck, err := ackPkt.Append([]byte{byte(wire.ApplicationPayloadKind_StreamAck)})
				if err != nil {
					s.logger.Error("failed to encode transfer ack", "error", err)
				} else {
					ack = encodedAck
				}
			}
		}
		updateWindowStream := s.updateWindow.Pop()
		cancelStream := s.cancelTrigger.Pop()
		stream := s.sendTrigger.Pop()
		if stream != nil && stream.Completed() {
			s.removeSendStream(stream.id)
			stream = nil
		}
		if !s.sh.CanSend() && !isPTO {
			if ack != nil {
				s.send.Push(&SendAction{
					base: s,
					ACK:  ack,
				})
			}
			// TODO: more efficient re-pushing
			if cancelStream != nil && !cancelStream.EOF() {
				s.cancelTrigger.Push(cancelStream) // re-push
			}
			if updateWindowStream != nil && !updateWindowStream.EOF() {
				s.updateWindow.Push(updateWindowStream) // re-push
			}
			// Do NOT re-push the congestion-blocked data stream onto sendTrigger
			// here. Push() fires the trigger notification, which the run loop's
			// own select observes immediately — so the loop wakes, finds
			// CanSend() still false, re-pushes, wakes again: a 0-delay busy-spin
			// (~150k iters/s, measured). On a multi-threaded host it merely burns
			// a core until an ACK opens the window; on the single-threaded wasm
			// runtime the spin starves the JS event loop, so inbound ACKs/pongs
			// are never delivered — cwnd never opens and the upload wedges the
			// whole WebUI until the connection dies by idle timeout. The stream
			// is revived without a self-notify: onACK (send_stream.go) re-pushes
			// it when an ACK frees the window, detectLost re-pushes on loss, and
			// PTO (isPTO above) bypasses this block — and a congestion-blocked
			// stream always has bytesInFlight>=cwnd>0, so one of those always
			// fires.
			continue
		}
		if cancelStream != nil && !cancelStream.EOF() {
			pn := s.pnIssuer.ConsumePacketNumber()
			s.sh.OnSent(&SentPacket{
				Kind:         wire.ApplicationPayloadKind_StreamCancel,
				PacketNumber: pn,
				StreamID:     cancelStream.id,
				PacketSize:   fixedOverhead + payloadOverhead,
				SentTime:     time.Now(),
				OnLost: func(now time.Time) {
					if cancelStream.EOF() {
						return
					}
					s.cancelTrigger.Push(cancelStream)
				},
			})
			encodedID, ok := wire.EncodeVarint(uint64(cancelStream.id))
			if !ok {
				s.logger.Error("failed to encode stream ID", "stream_id", cancelStream.id)
				continue
			}
			encodedCancel, err := (&wire.CancelStreamPacket{
				Id: encodedID,
			}).Append([]byte{byte(wire.ApplicationPayloadKind_StreamCancel)})
			if err != nil {
				s.logger.Error("failed to encode cancel stream packet", "stream_id", cancelStream.id, "error", err)
				continue
			}
			s.send.Push(&SendAction{
				base:         s,
				PacketNumber: pn,
				Data:         encodedCancel,
				ACK:          ack,
			})
			if stream != nil {
				s.sendTrigger.Push(stream) // re-push
			}
			if updateWindowStream != nil {
				s.updateWindow.Push(updateWindowStream) // re-push
			}
			continue
		}
		if updateWindowStream != nil && !updateWindowStream.EOF() {
			newWindow := updateWindowStream.Window()
			pn := s.pnIssuer.ConsumePacketNumber()
			s.sh.OnSent(&SentPacket{
				Kind: wire.ApplicationPayloadKind_StreamWindowUpdate,
				OnACK: func(now time.Time) {
					s.logger.Debug("Peer updated window size", "new_window", newWindow)
					updateWindowStream.onWindowAck(newWindow)
				},
				OnLost: func(now time.Time) {
					if updateWindowStream.EOF() {
						return
					}
					s.updateWindow.Push(updateWindowStream)
				},
				PacketNumber: pn,
				StreamID:     updateWindowStream.id,
				PacketSize:   fixedOverhead + payloadOverhead,
				SentTime:     time.Now(),
			})
			encodedID, ok := wire.EncodeVarint(uint64(updateWindowStream.id))
			if !ok {
				s.logger.Error("failed to encode stream ID", "stream_id", updateWindowStream.id)
				continue
			}
			encodedSize, ok := wire.EncodeVarint(uint64(newWindow))
			if !ok {
				s.logger.Error("failed to encode window size", "window_size", newWindow)
				continue
			}
			encodedWindow, err := (&wire.UpdateWindow{
				Id:        encodedID,
				WindowMax: encodedSize,
			}).Append([]byte{byte(wire.ApplicationPayloadKind_StreamWindowUpdate)})
			if err != nil {
				s.logger.Error("failed to encode update window packet", "stream_id", updateWindowStream.id, "window_size", newWindow, "error", err)
				continue
			}
			s.send.Push(&SendAction{
				base:         s,
				PacketNumber: pn,
				Data:         encodedWindow,
				ACK:          ack,
			})
			if stream != nil {
				s.sendTrigger.Push(stream) // re-push
			}
			continue
		}
		if stream != nil {
			maxPayload := s.mtu.CurrentMTU() - fixedOverhead
			sentRange := stream.triggerPacket(maxPayload)
			if sentRange != nil {
				pn := s.pnIssuer.ConsumePacketNumber()
				s.sh.OnSent(&SentPacket{
					Kind:         wire.ApplicationPayloadKind_StreamData,
					OnACK:        sentRange.OnACK,
					OnLost:       sentRange.OnLost,
					PacketNumber: pn,
					StreamID:     stream.id,
					PacketSize:   fixedOverhead + payloadOverhead + len(sentRange.Data),
					SentTime:     time.Now(),
				})
				pkt := wire.StreamPacket{}
				if stream.id != 0 {
					encodedID, ok := wire.EncodeVarint(uint64(stream.id))
					if !ok {
						s.logger.Error("failed to encode stream ID", "stream_id", stream.id)
						continue
					}
					pkt.SetHasId(true)
					pkt.SetId(encodedID)
				}
				if sentRange.Offset != 0 {
					encodedOffset, ok := wire.EncodeVarint(sentRange.Offset)
					if !ok {
						s.logger.Error("failed to encode stream offset", "stream_id", stream.id, "offset", sentRange.Offset)
						continue
					}
					pkt.SetHasOffset(true)
					pkt.SetOffset(encodedOffset)
				}
				pkt.SetIsEof(sentRange.Eof)
				pkt.Data = sentRange.Data
				encodedPkt, err := pkt.Append([]byte{byte(wire.ApplicationPayloadKind_StreamData)})
				if err != nil {
					s.logger.Error("failed to encode stream packet", "stream_id", stream.id, "error", err)
					continue
				}
				s.send.Push(&SendAction{
					base:         s,
					PacketNumber: pn,
					Data:         encodedPkt,
					ACK:          ack,
				})
			}
		}

		///*
		probeTime := time.Now()
		if probe := s.mtu.Probe(probeTime); probe != -1 {
			s.logger.Debug("sending MTU probe", "size", probe)
			pn := s.pnIssuer.ConsumePacketNumber()
			s.sh.OnSent(&SentPacket{
				Kind:         wire.ApplicationPayloadKind_StreamData,
				PacketNumber: pn,
				PacketSize:   probe,
				SentTime:     probeTime,
				IsMTUProbe:   true,
				StreamID:     0,
				OnACK: func(now time.Time) {
					s.mtu.OnACK(now)
				},
				OnLost: func(now time.Time) {
					s.mtu.OnLost(now)
				},
			})
			pkt := wire.StreamPacket{}
			pkt.SetIsProbe(true)
			pkt.Data = make([]byte, probe-fixedOverhead) // fill the packet to the probed MTU size
			data, err := pkt.Append([]byte{byte(wire.ApplicationPayloadKind_StreamData)})
			if err != nil {
				s.logger.Error("failed to create MTU probe packet", "error", err)
				continue
			}
			s.send.Push(&SendAction{
				base:         s,
				PacketNumber: pn,
				Data:         data,
				ACK:          ack,
			})
			continue
		}
		//*/
		if ack != nil {
			s.send.Push(&SendAction{
				base: s,
				ACK:  ack,
			})
		}
	}
}

func (s *Streams) Send(msg *objproto.Message) {
	s.recv.Push(msg)
}

func (s *Streams) Recv(ctx context.Context) *SendAction {
	for {
		action := s.send.Pop()
		if action != nil {
			return action
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.send.Notification():
		}
	}
}

const DefaultInitialMTU = 1200
const DefaultMaxMTU = 1500

func NewStreams(ctx context.Context, isServer bool, initialMTU int, maxMTU int, pnIssuer PacketNumberIssuer, logger *slog.Logger) Transport {
	s := &Streams{
		ctx:                ctx,
		sendStreams:        make(map[StreamID]*sendStream),
		recvStreams:        make(map[StreamID]*recvStream),
		sendTrigger:        newWithTriggerQueue[sendStream](),
		updateWindow:       newWithTriggerQueue[recvStream](),
		cancelTrigger:      newWithTriggerQueue[recvStream](),
		recv:               newWithTriggerQueue[objproto.Message](),
		send:               newWithTriggerQueue[SendAction](),
		newRecvStreamQueue: make(chan ReceiveStream, 100),
		newBidiStreamQueue: make(chan BidirectionalStream, 100),
		isServer:           isServer,
		logger:             logger,
		pnIssuer:           pnIssuer,
		mtu:                mtu.NewMTUTracker(initialMTU, maxMTU, 30*time.Second),
	}
	if isServer {
		s.bidiIDIssuer = NewIDIssuer(ServerBidirectionalStart)
		s.uniIDIssuer = NewIDIssuer(ServerUnidirectionalStart)
	} else {
		s.bidiIDIssuer = NewIDIssuer(ClientBidirectionalStart)
		s.uniIDIssuer = NewIDIssuer(ClientUnidirectionalStart)
	}
	rtt := congestion.NewRTTStats(333 * time.Millisecond)
	s.sh = NewSentPacketHandler(logger, rtt, congestion.NewNewReno(s.mtu, rtt, logger))
	s.pt = NewPacketNumTracker()
	go s.run(ctx)
	return s
}

type bidiStream struct {
	*sendStream
	*recvStream
}

func (b *bidiStream) CloseBoth() error {
	b.recvStream.Cancel()
	return b.sendStream.Close()
}

func (r *Streams) CreateBidirectionalStream() BidirectionalStream {
	r.streamsLock.Lock()
	defer r.streamsLock.Unlock()
	id := StreamID(0)
	id = r.bidiIDIssuer.Next()
	ss := newSendStream(r.ctx, r.mtu, id, newFlowController(InitialFlowWindow), r.logger, r.sendTrigger)
	rs := newReceiveStream(r.ctx, r.logger, id, InitialFlowWindow, r.updateWindow, r.cancelTrigger)
	r.sendStreams[ss.ID()] = ss
	r.recvStreams[ss.ID()] = rs
	// Advertise creation: queue a 0-byte STREAM frame so the peer's recv
	// path materializes the stream entry. Without this, peers can't resolve
	// an idle freshly-created stream via GetBidirectionalStream(id).
	ss.pendingOpen.Store(true)
	r.sendTrigger.Push(ss)
	return &bidiStream{
		sendStream: ss,
		recvStream: rs,
	}
}

func (r *Streams) CreateSendStream() SendStream {
	r.streamsLock.Lock()
	defer r.streamsLock.Unlock()
	id := StreamID(0)
	id = r.uniIDIssuer.Next()
	ss := newSendStream(r.ctx, r.mtu, id, newFlowController(InitialFlowWindow), r.logger, r.sendTrigger)
	r.sendStreams[ss.ID()] = ss
	ss.pendingOpen.Store(true)
	r.sendTrigger.Push(ss)
	return ss
}

type wrapRecvStream struct {
	*recvStream
}

func (w *wrapRecvStream) ID() StreamID {
	return w.recvStream.id
}
func (r *Streams) AcceptReceiveStream(ctx context.Context) (ReceiveStream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case rs := <-r.newRecvStreamQueue:
		return rs, nil
	}
}

func (r *Streams) AcceptBidirectionalStream(ctx context.Context) (BidirectionalStream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case bs := <-r.newBidiStreamQueue:
		return bs, nil
	}
}
