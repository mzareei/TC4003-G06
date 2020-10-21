package chandy_lamport

import (
	"fmt"
	"log"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	snapshots 	  *SyncMap // A safe Mapping mechanism that contains a mapping from servers Ids and a SnapshotState
}

type Snapshot struct {
	state SnapshotState
	receivedMarkers map[string]bool
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		NewSyncMap(),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	snapShotMessage := SnapshotMessage{src, server.Id, message}

	// Identifying the message type and taking action accordingly.
	switch message := message.(type) {
		// Case for tokens representing the process state.
		case TokenMessage:
			fmt.Println(server.Id, ": Receives TokenMessage from", src)

			// Get the number of tokens in the process.
			numTokens := message.numTokens

			// The server receives the number of tokens.
			server.Tokens += numTokens

			// The server remembers whether it has been snapshotted.
			server.snapshots.Range(func (key, value interface{}) bool {
				snapshot := value.(*Snapshot)
				if _, ok := snapshot.receivedMarkers[src]; !ok {
					snapshot.state.messages = append(snapshot.state.messages, &snapShotMessage)
				}
				return true
			})

		// Case for marker messages.
		case MarkerMessage:
			fmt.Println(server.Id, ": Receives MarkerMessage from", src)

			// Get the snapshotId received in the message.
			snapshotID := message.snapshotId
			value, snapshotOccurred := server.snapshots.Load(snapshotID)
			// Check whether there is a snapshot of the received snapshotID in the server.
			if !snapshotOccurred {
				// If there is no a snapshot, take the snapshot.
				server.StartSnapshot(snapshotID)
				value, _ = server.snapshots.Load(snapshotID)
			}
			snapshot := value.(*Snapshot)

			snapshot.receivedMarkers[src] = true
			if len(snapshot.receivedMarkers) == len(server.inboundLinks) {
				server.sim.NotifySnapshotComplete(server.Id, message.snapshotId)
			}
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	fmt.Println(server.Id,": StartSnapshot snapshotId", snapshotId)

	// Create tokens map, mapping keys to expected token Ids.
	serverTokens := make(map[string]int)
	serverTokens[server.Id] = server.Tokens

	// Create server messages array.
	serverMessages := make([]*SnapshotMessage, 0)

	// Stores own snapshot in the snapshots map of the server.
	snapshotState := SnapshotState{snapshotId, serverTokens, serverMessages}
	snapshot := Snapshot{snapshotState, make(map[string]bool)}
	server.snapshots.Store(snapshotId, &snapshot)

	// Send a marker message to all server outbound links, to take their snapshots.
	server.SendToNeighbors(MarkerMessage{snapshotId})
}
