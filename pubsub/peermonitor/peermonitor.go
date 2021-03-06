package peermonitor

import (
	"context"
	"time"

	"berty.tech/go-orbit-db/events"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.uber.org/zap"
)

// NewPeerMonitorOptions Options for creating a new PeerMonitor instance
type NewPeerMonitorOptions struct {
	Start        *bool
	PollInterval *time.Duration
}

func durationPtr(duration time.Duration) *time.Duration {
	return &duration
}

func boolPtr(val bool) *bool {
	return &val
}

var defaultPeerMonitorOptions = &NewPeerMonitorOptions{
	Start:        boolPtr(true),
	PollInterval: durationPtr(time.Second),
}

type peerMonitor struct {
	events.EventEmitter
	cancelFunc   func()
	ipfs         coreapi.CoreAPI
	topic        string
	started      bool
	pollInterval time.Duration
	peers        map[peer.ID]struct{}
}

func (p *peerMonitor) Start(ctx context.Context) func() {
	if p.started == true {
		p.Stop()
	}

	ctx, cancelFunc := context.WithCancel(ctx)

	p.started = true
	p.cancelFunc = cancelFunc

	go func() {
		for {
			select {
			case <-ctx.Done():
				p.cancelFunc = nil
				p.started = false
				return

			case <-time.After(p.pollInterval):
				err := p.pollPeers(ctx)
				if err != nil {
					logger().Error("error while polling peers", zap.Error(err))
				}

				break
			}
		}
	}()

	return cancelFunc
}

func (p *peerMonitor) Stop() {
	if p.cancelFunc == nil {
		return
	}

	p.cancelFunc()
}

func (p *peerMonitor) GetPeers() []peer.ID {
	var peerIDs []peer.ID
	for p := range p.peers {
		peerIDs = append(peerIDs, p)
	}

	return peerIDs
}

func (p *peerMonitor) HasPeer(id peer.ID) bool {
	_, ok := p.peers[id]

	return ok
}

func (p *peerMonitor) pollPeers(ctx context.Context) error {
	peerIDs, err := p.ipfs.PubSub().Peers(ctx, options.PubSub.Topic(p.topic))

	currentPeers := map[peer.ID]struct{}{}
	allPeers := map[peer.ID]struct{}{}
	newPeers := map[peer.ID]struct{}{}

	for peerID := range p.peers {
		currentPeers[peerID] = struct{}{}
	}

	if err != nil {
		return err
	}

	for _, peerID := range peerIDs {
		allPeers[peerID] = struct{}{}

		if _, ok := currentPeers[peerID]; ok {
			delete(currentPeers, peerID)
			p.Emit(NewEventPeerJoin(peerID))
		} else if _, ok := allPeers[peerID]; !ok {
			newPeers[peerID] = struct{}{}
			p.Emit(NewEventPeerLeave(peerID))
		}
	}

	p.peers = allPeers

	return nil
}

// NewPeerMonitor Creates a new PeerMonitor instance
func NewPeerMonitor(ctx context.Context, ipfs coreapi.CoreAPI, topic string, options *NewPeerMonitorOptions) Interface {
	if options == nil {
		options = defaultPeerMonitorOptions
	}

	if options.PollInterval == nil {
		options.PollInterval = defaultPeerMonitorOptions.PollInterval
	}

	if options.Start == nil {
		options.Start = defaultPeerMonitorOptions.Start
	}

	monitor := &peerMonitor{
		ipfs:         ipfs,
		topic:        topic,
		pollInterval: *options.PollInterval,
	}

	if *options.Start == true {
		monitor.Start(ctx)
	}

	return monitor
}

var _ Interface = &peerMonitor{}
