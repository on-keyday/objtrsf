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

	// congestionBlocked holds send streams that were popped from sendTrigger
	// while the connection was congestion-blocked (CanSend()==false). They are
	// parked here instead of re-pushed (re-pushing self-notifies and busy-spins)
	// and drained back into sendTrigger by the run loop once cwnd reopens. Run
	// loop-confined: only run() touches it, so it needs no lock.
	congestionBlocked map[StreamID]*sendStream

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

	// Where the loop's wall-clock actually goes. loopIter says how FAST the
	// loop turns; these say why it is not turning faster, which is a different
	// question and the one a transfer running far below its congestion window
	// poses. Measured on a 2 ms path: ~1,850 iterations/s at one packet each,
	// 540 µs per packet, on a host 79% idle — the loop was not computing, and
	// nothing here could say what it was waiting for.
	//
	// blocks counts parks ENTERED. blockedNs accrues on the wake AND includes
	// the park still in progress, added by the reader from parkStartNs: a loop
	// parked for a whole sampling interval would otherwise report zero blocked
	// time, which on the operator's screen reads as "busy" — the exact opposite
	// of the truth, and the first thing this instrument printed. The reader
	// pays one clock read per CALL for that, not per park.
	//
	// wakeTimer and wakeSend are the two arms the diagnosis turns on — a park
	// ended by the timer is the transport waiting on a deadline of its own, one
	// ended by the send trigger is the application not feeding it. Everything
	// else (an inbound packet, an ACK to send, a window update, a cancel) is
	// the remainder, blocks - wakeTimer - wakeSend, and means the peer.
	//
	// armedPacer splits the timer arm: the pacer's own floor is 1 ms, so a loop
	// held by it and a loop held by loss detection look identical in wakeTimer
	// and call for opposite fixes.
	blockedNs atomic.Uint64
	blocks    atomic.Uint64
	// parkStartNs is the monotonic-ish start of the park in progress (0 when
	// the loop is running). A reader adds now-parkStartNs to blockedNs; a wake
	// racing that read costs at most one sample's worth of skew, which for a
	// counter read at second-scale intervals is not worth a lock.
	parkStartNs atomic.Int64
	wakeTimer   atomic.Uint64
	wakeSend    atomic.Uint64
	armedPacer  atomic.Uint64

	// recvStreamGraceNs is the delay (nanoseconds) between a recv stream's
	// removal trigger (EOF frame received, or a local cancel sent) and its
	// recvStreams map entry being dropped. While the entry lives, late
	// duplicate/retransmitted packets for the stream are routed to it and
	// deduplicated; after removal a stray packet would materialize a fresh
	// entry via getRecvStream. Default one minute (≫ any RTO).
	recvStreamGraceNs atomic.Int64
}

// SetRecvStreamRemovalGrace overrides the grace period between a recv
// stream's removal trigger and its map entry being dropped. Intended for
// tests (the production default is one minute).
func (s *Streams) SetRecvStreamRemovalGrace(d time.Duration) {
	s.recvStreamGraceNs.Store(int64(d))
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
	// Loss is the loss detector's account of itself. Spurious rising during a
	// transfer means the congestion response is being driven by packets that
	// were only late — see LossStats.
	Loss LossStats

	// The run loop's account of its own waiting. All cumulative, all meaningful
	// only as a DELTA between two readings; see the field comments on Streams
	// for what each one distinguishes.
	//
	// The two derived numbers worth naming, because they are what a reader
	// actually wants:
	//
	//	duty cycle = BlockedNs / wall time between two readings
	//	mean park  = BlockedNs / Blocks
	//
	// and LoopIterations - Blocks is the count of iterations that never parked
	// at all — the received-packet fast path at the top of the loop.
	BlockedNs  uint64
	Blocks     uint64
	WakeTimer  uint64
	WakeSend   uint64
	ArmedPacer uint64
}

func (s *Streams) GetInternalState() *InternalState {
	s.streamsLock.Lock()
	activeSendStream := len(s.sendStreams)
	activeRecvStream := len(s.recvStreams)
	s.streamsLock.Unlock()
	sentRanges, bytesInFlight, congestionWindow, smoothedRTT, rttVariance, lossStats := s.sh.GetInternal()
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
		Loss:                 lossStats,
		BlockedNs:            s.blockedNsNow(),
		Blocks:               s.blocks.Load(),
		WakeTimer:            s.wakeTimer.Load(),
		WakeSend:             s.wakeSend.Load(),
		ArmedPacer:           s.armedPacer.Load(),
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

// GetBidirectionalStream resolves a bidirectional id for as long as EITHER half
// is still live, standing a finished stub in for one that has been reaped.
//
// Requiring both was an intersection of two DIFFERENT lifetimes, so a lookup
// died with the shorter one. recvStreams is dropped on a grace timer after EOF
// — deliberately, so late retransmits stay routable — while sendStreams is
// dropped as soon as the run loop sees Completed(). Worse, the peer's CloseBoth
// cancels the local send half, which drives it to Completed within
// milliseconds: a remote close could revoke the local caller's only way to
// reach bytes still sitting unread in recvStreams.
//
// A missing half always means FINISHED, never "not created yet": both halves
// are made together, by CreateBidirectionalStream locally and by getRecvStream
// for a peer-initiated stream. So the stub reports its direction as over —
// reads EOF, refuses bytes — rather than pretending the stream is fresh.
//
// Unidirectional ids are refused outright. They legitimately have exactly one
// half, and composing a stub for the other would hand back a bidirectional view
// of a stream that is not one. The old both-halves rule excluded them as a side
// effect; relaxing it makes the check load-bearing.
func (s *Streams) GetBidirectionalStream(id StreamID) BidirectionalStream {
	if !id.IsBidirectional() {
		return nil
	}
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	rs, hasRecv := s.recvStreams[id]
	sd, hasSend := s.sendStreams[id]
	if !hasRecv && !hasSend {
		return nil
	}
	b := &bidiStream{SendStream: finishedSendStream{id}, ReceiveStream: finishedRecvStream{id}}
	if hasSend {
		b.SendStream = sd
	}
	if hasRecv {
		b.ReceiveStream = &wrapRecvStream{rs}
	}
	return b
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
					SendStream:    sd,
					ReceiveStream: &wrapRecvStream{rs},
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

// scheduleRecvStreamRemoval drops the stream's recvStreams entry after the
// removal grace period. Triggered when the EOF frame is received or a local
// cancel is sent — the two events after which only late duplicates can still
// arrive for the stream. Idempotent per stream (removalScheduled).
func (s *Streams) scheduleRecvStreamRemoval(rs *recvStream) {
	if !rs.markRemovalScheduled() {
		return
	}
	time.AfterFunc(time.Duration(s.recvStreamGraceNs.Load()), func() {
		s.removeRecvStream(rs.id)
	})
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
		// Schedule removal when the EOF frame is RECEIVED. Gating on rs.EOF()
		// alone never fires here: it reports eofReached, which is set by the
		// application reader consuming the EOF — after which no further packet
		// arrives on this stream to re-evaluate the gate, so the map entry
		// leaked one recvStream per completed inbound stream (fleet-wide
		// ~6.4MB/day under a 5s scrape). The reader is unaffected by removal:
		// it holds a direct pointer, and the grace period keeps the entry
		// routable for late retransmits.
		if data.IsEof() || rs.EOF() {
			s.scheduleRecvStreamRemoval(rs)
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

// wakeReason says what ended a park in the run loop's select.
//
// It exists so each select arm can record a value instead of carrying its own
// accounting and its own `continue`: the two arms that resume at the top of the
// loop (a received packet, a loss-timer reset) now say so through this type,
// and the single decision after the select does what they used to do inline.
type wakeReason uint8

const (
	wokeNothing wakeReason = iota
	wokeTimer
	wokeSend
	wokeWindow
	wokeCancel
	wokeAck
	wokeRecv
	wokeSentHandler
)

// resumesLoop reports whether this wake means "start the iteration over"
// rather than "go on to the send half". Only the two arms that used to hold a
// bare `continue`.
func (r wakeReason) resumesLoop() bool {
	return r == wokeRecv || r == wokeSentHandler
}

// noteWake records one completed park. Called once per park — so the clock read
// it costs is paid per park, not per packet — and after the select, so the
// duration covers the whole wait.
func (s *Streams) noteWake(parkStart time.Time, r wakeReason) {
	s.parkStartNs.Store(0)
	s.blockedNs.Add(uint64(time.Since(parkStart)))
	switch r {
	case wokeTimer:
		s.wakeTimer.Add(1)
	case wokeSend:
		s.wakeSend.Add(1)
	}
}

// nextWakeDeadline computes the wall-clock time the run loop should wake to do
// timer-driven work (loss detection / pacing), independent of the I/O
// notification channels it also selects on. A zero time means "no timer; block
// until a notification arrives".
//
// The second return says whether the deadline returned is the PACER's rather
// than loss detection's. The counter built on it is the only thing that
// separates a loop rate-limited by its own bandwidth estimate from one waiting
// on a peer that has gone quiet: both park on a timer, and a wake count alone
// cannot tell them apart.
func (s *Streams) nextWakeDeadline() (time.Time, bool) {
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
			return pacer, true
		}
	}
	return deadline, false
}

func (s *Streams) run(ctx context.Context) {
	// One timer for the life of the loop. This used to be time.After inside
	// the select below, which allocates a fresh timer on every iteration and
	// never stops the ones another case wins -- 17.5% of the process's
	// allocations on a bulk transfer, at roughly one loop iteration per
	// packet. Reset is enough on its own here: since Go 1.23 a timer channel
	// is unbuffered and Reset discards any value that was sent but not
	// received, so an armed-but-unread timer from a previous iteration cannot
	// leak into this one.
	wake := time.NewTimer(time.Hour)
	wake.Stop()
	defer wake.Stop()
	for {
		s.loopIter.Add(1)
		// Revive congestion-blocked streams now that cwnd has reopened. These
		// were parked (not re-pushed) while CanSend()==false to avoid the
		// 0-delay self-notify busy-spin; the event that grows cwnd is an inbound
		// ACK (handlePacket below), so re-pushing here — only when CanSend() is
		// true — wakes the loop into a real send, not another blocked spin. This
		// is required because a stream blocked before its first transmission has
		// no in-flight packet of its own, so the per-stream onACK/onLost revival
		// can never fire for it; without this drain it would orphan forever.
		if len(s.congestionBlocked) > 0 && s.sh.CanSend() {
			for id, blocked := range s.congestionBlocked {
				delete(s.congestionBlocked, id)
				s.sendTrigger.Push(blocked)
			}
		}
		recvedData := s.recv.Pop()
		if recvedData != nil {
			s.handlePacket(recvedData)
			continue
		}
		deadline, fromPacer := s.nextWakeDeadline()
		// Count the park as it is ENTERED: a loop that never wakes again still
		// reports having parked, and a frozen loopIter beside it says it is
		// still there.
		s.blocks.Add(1)
		if fromPacer {
			s.armedPacer.Add(1)
		}
		parkStart := time.Now()
		s.parkStartNs.Store(parkStart.UnixNano())
		woke := wokeNothing
		if deadline.IsZero() {
			select {
			case <-ctx.Done():
				return // end
			case <-s.sendTrigger.Notification(): // when new data to send
				woke = wokeSend
			case <-s.updateWindow.Notification(): // when new recv window to update
				woke = wokeWindow
			case <-s.cancelTrigger.Notification(): // when stream cancel is requested
				woke = wokeCancel
			case <-s.pt.NotifyReceive(): // when new stream ack to process
				woke = wokeAck
			case <-s.recv.Notification(): // when new data received
				woke = wokeRecv
			case <-s.sh.Notification(): // when timer resets
				woke = wokeSentHandler
			}
		} else {
			wake.Reset(time.Until(deadline))
			select {
			case <-ctx.Done():
				return // end
			case <-wake.C:
				woke = wokeTimer
			case <-s.sendTrigger.Notification(): // when new data to send
				woke = wokeSend
			case <-s.updateWindow.Notification(): // when new recv window to update
				woke = wokeWindow
			case <-s.cancelTrigger.Notification(): // when stream cancel is requested
				woke = wokeCancel
			case <-s.pt.NotifyReceive(): // when new stream ack to process
				woke = wokeAck
			case <-s.recv.Notification(): // when new data received
				woke = wokeRecv
			case <-s.sh.Notification(): // when timer resets
				woke = wokeSentHandler
			}
		}
		s.noteWake(parkStart, woke)
		if woke.resumesLoop() {
			// A received packet is processed immediately at the top of the
			// loop; a loss-timer reset re-evaluates the deadline. Neither runs
			// the send half.
			continue
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
			// Park (do NOT re-push) the congestion-blocked data stream. Re-pushing
			// onto sendTrigger fires the trigger notification, which the run loop's
			// own select observes immediately — so the loop wakes, finds CanSend()
			// still false, re-pushes, wakes again: a 0-delay busy-spin (~150k
			// iters/s, measured). On a multi-threaded host it merely burns a core
			// until an ACK opens the window; on the single-threaded wasm runtime
			// the spin starves the JS event loop, so inbound ACKs/pongs are never
			// delivered — cwnd never opens and the upload wedges the whole WebUI
			// until the connection dies by idle timeout.
			//
			// Instead, hold the stream in congestionBlocked and let the drain at
			// the top of the loop re-push it once an inbound ACK has grown cwnd
			// (CanSend() true) — a real external event, so no self-notify spin.
			// The earlier version dropped the stream entirely, trusting the
			// per-stream onACK/detectLost/PTO to revive it. That holds only for a
			// stream with an in-flight packet of its own; a stream popped here
			// before it ever transmitted (e.g. a later stream in a multi-stream
			// batch that earlier streams already filled cwnd against) has none, so
			// none of those callbacks ever fire for it and it orphans forever.
			if stream != nil {
				s.congestionBlocked[stream.id] = stream
			}
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
			// A cancelled stream never sees an EOF frame, so the EOF-received
			// path in handlePacket cannot reclaim its recvStreams entry.
			s.scheduleRecvStreamRemoval(cancelStream)
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
		congestionBlocked:  make(map[StreamID]*sendStream),
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
	s.recvStreamGraceNs.Store(int64(time.Minute))
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

// bidiStream joins the two halves of a bidirectional stream. The fields are
// INTERFACES rather than the concrete streams because a half may be a finished
// stub — see GetBidirectionalStream.
type bidiStream struct {
	SendStream
	ReceiveStream
}

// ID resolves what would otherwise be an ambiguous selector: both embedded
// interfaces declare it, and both halves of a stream carry the same id.
func (b *bidiStream) ID() StreamID { return b.SendStream.ID() }

func (b *bidiStream) CloseBoth() error {
	b.ReceiveStream.Cancel()
	return b.SendStream.Close()
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
		SendStream:    ss,
		ReceiveStream: &wrapRecvStream{rs},
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

// blockedNsNow is blockedNs plus the park currently in progress.
//
// Without the second term a loop parked across a whole sampling interval
// reports zero blocked time — indistinguishable from one that never stopped,
// and the reading an operator is most likely to be looking at when they ask
// why nothing is moving.
func (s *Streams) blockedNsNow() uint64 {
	total := s.blockedNs.Load()
	if start := s.parkStartNs.Load(); start != 0 {
		if d := time.Now().UnixNano() - start; d > 0 {
			total += uint64(d)
		}
	}
	return total
}
