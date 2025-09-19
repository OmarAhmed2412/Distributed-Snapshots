package asg3

import (
	"log"
	"math/rand"
	"sync"
)

// Max random delay added to packet delivery
const maxDelay = 5

type ChandyLamportSim struct {
	time           int
	nextSnapshotId int
	nodes          map[string]*Node // key = node ID
	logger         *Logger
	// TODO: You can add more fields here.
	pendingSnapshots   map[int]map[string]bool
	completedSnapshots map[int]bool            
	mu                 sync.Mutex
	// snapshotCompletions map[int]chan struct{}
	snapshotWaitGroups map[int]*sync.WaitGroup
}

func NewSimulator() *ChandyLamportSim {
	return &ChandyLamportSim{
		time:           0,
		nextSnapshotId: 0,
		nodes:          make(map[string]*Node),
		logger:         NewLogger(),
		// ToDo: you may need to modify this if you modify the above struct
		pendingSnapshots:   make(map[int]map[string]bool),
        completedSnapshots: make(map[int]bool),
		// snapshotCompletions: make(map[int]chan struct{}),
		snapshotWaitGroups: make(map[int]*sync.WaitGroup),
	}
}

// Add a node to this simulator with the specified number of starting tokens
func (sim *ChandyLamportSim) AddNode(id string, tokens int) {
	node := CreateNode(id, tokens, sim)
	sim.nodes[id] = node
}

// Add a unidirectional link between two nodes
func (sim *ChandyLamportSim) AddLink(src string, dest string) {
	node1, ok1 := sim.nodes[src]
	node2, ok2 := sim.nodes[dest]
	if !ok1 {
		log.Fatalf("Node %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Node %v does not exist\n", dest)
	}
	node1.AddOutboundLink(node2)
}

func (sim *ChandyLamportSim) ProcessEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.nodes[event.src]
		src.SendTokens(event.tokens, event.dest)
	case SnapshotEvent:
		sim.StartSnapshot(event.nodeId)
	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step and deliver at most one packet per node
func (sim *ChandyLamportSim) Tick() {
	sim.time++
	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the nodes,
	// we must also iterate through the nodes and the links in a deterministic way
	for _, nodeId := range getSortedKeys(sim.nodes) {
		node := sim.nodes[nodeId]
		for _, dest := range getSortedKeys(node.outboundLinks) {
			link := node.outboundLinks[dest]
			// Deliver at most one packet per node at each time step to
			// establish total ordering of packet delivery to each node
			if !link.msgQueue.Empty() {
				e := link.msgQueue.Peek().(SendMsgEvent)
				if e.receiveTime <= sim.time {
					link.msgQueue.Pop()
					sim.logger.RecordEvent(
						sim.nodes[e.dest],
						ReceivedMsgRecord{e.src, e.dest, e.message})
					sim.nodes[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// Return the receive time of a message after adding a random delay.
// Note: At each time step, only one message is delivered to a destination.
// This means that the message may be received *after* the time step returned in this function.
// See the clarification in the document of the assignment
func (sim *ChandyLamportSim) GetReceiveTime() int {
	return sim.time + 1 + rand.Intn(maxDelay)
}

func (sim *ChandyLamportSim) StartSnapshot(nodeId string) {
	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++
	sim.logger.RecordEvent(sim.nodes[nodeId], StartSnapshotRecord{nodeId, snapshotId})
	// TODO: Complete this method
	sim.mu.Lock()
    defer sim.mu.Unlock()
    // Initialize WaitGroup for this snapshot
	wg := &sync.WaitGroup{}
    wg.Add(len(sim.nodes))
    sim.snapshotWaitGroups[snapshotId] = wg

	// Initialize pending tracking
    sim.pendingSnapshots[snapshotId] = make(map[string]bool)
    // sim.snapshotCompletions[snapshotId] = make(chan struct{})
    for id := range sim.nodes {
        sim.pendingSnapshots[snapshotId][id] = false
    }
    // Start the snapshot process at the initiating node
    sim.nodes[nodeId].StartSnapshot(snapshotId)

}

func (sim *ChandyLamportSim) NotifyCompletedSnapshot(nodeId string, snapshotId int) {
	sim.logger.RecordEvent(sim.nodes[nodeId], EndSnapshotRecord{nodeId, snapshotId})
	// TODO: Complete this method
    sim.mu.Lock()
    defer sim.mu.Unlock()

    // Mark this node as completed for this snapshot
    sim.pendingSnapshots[snapshotId][nodeId] = true
    
    // allCompleted := true
    // for _, completed := range sim.pendingSnapshots[snapshotId] {
    //     if !completed {
    //         allCompleted = false
    //         break
    //     }
    // }
    
    // if allCompleted {
    //     close(sim.snapshotCompletions[snapshotId])
    //     sim.completedSnapshots[snapshotId] = true
    // }
	
	// Wait for all nodes to complete
	if wg, exists := sim.snapshotWaitGroups[snapshotId]; exists {
        wg.Done()
    }


}

func (sim *ChandyLamportSim) CollectSnapshot(snapshotId int) *GlobalSnapshot {
	// TODO: Complete this method
	// Wait for completion
	sim.mu.Lock()
    wg := sim.snapshotWaitGroups[snapshotId]
    sim.mu.Unlock()
    wg.Wait()
	snap := GlobalSnapshot{snapshotId, make(map[string]int), make([]*MsgSnapshot, 0)}
	
	// <-sim.snapshotCompletions[snapshotId]

    // for {
    //     sim.mu.Lock()
    //     completed := sim.completedSnapshots[snapshotId]
    //     sim.mu.Unlock()
        
    //     if completed {
    //         break
    //     }
    // }

	// Collect token states
    for nodeId, node := range sim.nodes {
        node.mu.Lock()
        if tokens, exists := node.recordedState[snapshotId]; exists {
            snap.tokenMap[nodeId] = tokens
        }
        node.mu.Unlock()
    }
    
    // Collect in-transit messages
    for _, node := range sim.nodes {
        node.mu.Lock()
        if msgs, exists := node.inTransitMessages[snapshotId]; exists {
            for _, msgList := range msgs {
                snap.messages = append(snap.messages, msgList...)
            }
        }
        node.mu.Unlock()
    }

	// Clean up WaitGroup
    sim.mu.Lock()
    delete(sim.snapshotWaitGroups, snapshotId)
    sim.mu.Unlock()

	return &snap
}
