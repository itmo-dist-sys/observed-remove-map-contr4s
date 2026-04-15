package node

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

const antiEntropyInterval = 30 * time.Millisecond

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode

	mu sync.RWMutex

	allNodeIDs []string
	counter    uint64
	state      MapState
}

type stateSyncMessage struct {
	State MapState
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	peers := make([]string, len(allNodeIDs))
	copy(peers, allNodeIDs)
	sort.Strings(peers)

	return &CRDTMapNode{
		BaseNode:   hive.NewBaseNode(id),
		allNodeIDs: peers,
		state:      make(MapState),
	}
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	wasRunning := n.BaseNode.IsRunning()
	if err := n.BaseNode.Start(ctx); err != nil {
		return err
	}

	if !wasRunning {
		go n.runAntiEntropy()
	}

	return nil
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	n.counter++
	n.state[k] = StateEntry{
		Value: v,
		Version: Version{
			Counter: n.counter,
			NodeID:  n.ID(),
		},
	}
	n.mu.Unlock()

	n.broadcastState()
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	entry, ok := n.state[k]
	if !ok || entry.Tombstone {
		return "", false
	}

	return entry.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	n.counter++
	n.state[k] = StateEntry{
		Tombstone: true,
		Version: Version{
			Counter: n.counter,
			NodeID:  n.ID(),
		},
	}
	n.mu.Unlock()

	n.broadcastState()
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for k, remoteEntry := range remote {
		localEntry, ok := n.state[k]
		if !ok || isNewerVersion(remoteEntry.Version, localEntry.Version) {
			n.state[k] = remoteEntry
		}

		if remoteEntry.Version.Counter > n.counter {
			n.counter = remoteEntry.Version.Counter
		}
	}
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()

	copyState := make(MapState, len(n.state))
	for k, v := range n.state {
		copyState[k] = v
	}
	return copyState
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	view := make(map[string]string, len(n.state))
	for k, entry := range n.state {
		if entry.Tombstone {
			continue
		}
		view[k] = entry.Value
	}

	return view
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	switch payload := msg.Payload.(type) {
	case stateSyncMessage:
		n.Merge(payload.State)
	case *stateSyncMessage:
		n.Merge(payload.State)
	case MapState:
		n.Merge(payload)
	}

	return nil
}

func (n *CRDTMapNode) runAntiEntropy() {
	ticker := time.NewTicker(antiEntropyInterval)
	defer ticker.Stop()

	ctx := n.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.broadcastState()
		}
	}
}

func (n *CRDTMapNode) broadcastState() {
	snapshot := n.State()
	for _, id := range n.allNodeIDs {
		if id == n.ID() {
			continue
		}
		n.SendMessage(hive.NewMessage(n.ID(), id, stateSyncMessage{State: snapshot}))
	}
}

func isNewerVersion(a, b Version) bool {
	if a.Counter != b.Counter {
		return a.Counter > b.Counter
	}
	return a.NodeID > b.NodeID
}
