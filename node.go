package asg3

import (
	"log"
	"sync"
)

// The main participant of the distributed snapshot protocol.
// nodes exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one node to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.

type Node struct {
	sim           *ChandyLamportSim
	id            string
	tokens        int
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src

	// TODO: add more fields here (what does each node need to keep track of?)
	mu                sync.Mutex
	activeSnapshots   map[int]bool                      // snapshots in progress at this node
	recordedState     map[int]int                       // recorded local state of tokens for each snapshot
	markedChannels    map[int]map[string]bool           // channels marked for each snapshot
	inTransitMessages map[int]map[string][]*MsgSnapshot // messages recorded for each snapshot
	snapshotCompleted map[int]bool
}

// A unidirectional communication channel between two nodes
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src      string
	dest     string
	msgQueue *Queue
}

func CreateNode(id string, tokens int, sim *ChandyLamportSim) *Node {
	return &Node{
		sim:           sim,
		id:            id,
		tokens:        tokens,
		outboundLinks: make(map[string]*Link),
		inboundLinks:  make(map[string]*Link),
		// TODO: You may need to modify this if you make modifications above
		activeSnapshots:   make(map[int]bool),
        recordedState:     make(map[int]int),
        markedChannels:    make(map[int]map[string]bool),
        inTransitMessages: make(map[int]map[string][]*MsgSnapshot),
        snapshotCompleted: make(map[int]bool),

	}
}

// Add a unidirectional link to the destination node
func (node *Node) AddOutboundLink(dest *Node) {
	if node == dest {
		return
	}
	l := Link{node.id, dest.id, NewQueue()}
	node.outboundLinks[dest.id] = &l
	dest.inboundLinks[node.id] = &l
}

// Send a message on all of the node's outbound links
func (node *Node) SendToNeighbors(message Message) {
	for _, nodeId := range getSortedKeys(node.outboundLinks) {
		link := node.outboundLinks[nodeId]
		node.sim.logger.RecordEvent(
			node,
			SentMsgRecord{node.id, link.dest, message})
		link.msgQueue.Push(SendMsgEvent{
			node.id,
			link.dest,
			message,
			node.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this node
func (node *Node) SendTokens(numTokens int, dest string) {
	if node.tokens < numTokens {
		log.Fatalf("node %v attempted to send %v tokens when it only has %v\n",
			node.id, numTokens, node.tokens)
	}
	message := Message{isMarker: false, data: numTokens}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
	// Update local state before sending the tokens
	node.tokens -= numTokens
	link, ok := node.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from node %v\n", dest, node.id)
	}

	link.msgQueue.Push(SendMsgEvent{
		node.id,
		dest,
		message,
		node.sim.GetReceiveTime()})
}

func (node *Node) HandlePacket(src string, message Message) {
	// TODO: Write this method
	node.mu.Lock()
    defer node.mu.Unlock()

    if message.isMarker {
        snapshotId := message.data
        // Initialize snapshot state if it doesn't exist
        if _, exists := node.activeSnapshots[snapshotId]; !exists {
            node.activeSnapshots[snapshotId] = true
            node.recordedState[snapshotId] = node.tokens
            // Initialize channel tracking
            node.markedChannels[snapshotId] = make(map[string]bool)
            node.inTransitMessages[snapshotId] = make(map[string][]*MsgSnapshot)
            for inSrc := range node.inboundLinks {
                node.markedChannels[snapshotId][inSrc] = false
                node.inTransitMessages[snapshotId][inSrc] = []*MsgSnapshot{}
            }
            // Send markers to neighbors (unlock during network operation)
            node.SendToNeighbors(Message{isMarker: true, data: snapshotId})
        }
        
        // Mark this channel as received
        if channels, exists := node.markedChannels[snapshotId]; exists {
            channels[src] = true
        }
        
        // Check if all channels are marked
        allMarked := true
        for _, marked := range node.markedChannels[snapshotId] {
            if !marked {
                allMarked = false
                break
            }
        }
        
        if allMarked && !node.snapshotCompleted[snapshotId] {
            node.snapshotCompleted[snapshotId] = true
            node.sim.NotifyCompletedSnapshot(node.id, snapshotId)
        }
    } else {
        // Token message
        node.tokens += message.data
        
        // Record for active snapshots where channel isn't marked
        for sid, active := range node.activeSnapshots {
            if active {
                if marked, exists := node.markedChannels[sid][src]; exists && !marked {
                    msg := &MsgSnapshot{
                        src:     src,
                        dest:    node.id,
                        message: message,
                    }
                    node.inTransitMessages[sid][src] = append(node.inTransitMessages[sid][src], msg)
                }
            }
        }
    }
}

func (node *Node) StartSnapshot(snapshotId int) {
	// ToDo: Write this method
	node.mu.Lock()
    defer node.mu.Unlock()
    
	// Initialization
    node.activeSnapshots[snapshotId] = true
    node.recordedState[snapshotId] = node.tokens
    node.markedChannels[snapshotId] = make(map[string]bool)
    for inSrc := range node.inboundLinks {
        node.markedChannels[snapshotId][inSrc] = false
    }
    node.inTransitMessages[snapshotId] = make(map[string][]*MsgSnapshot)
    for inSrc := range node.inboundLinks {
        node.inTransitMessages[snapshotId][inSrc] = []*MsgSnapshot{}
    }
    node.SendToNeighbors(Message{isMarker: true, data: snapshotId})
    // If no inbound channels
    if len(node.inboundLinks) == 0 {
        node.snapshotCompleted[snapshotId] = true
        node.sim.NotifyCompletedSnapshot(node.id, snapshotId)
    }
}
