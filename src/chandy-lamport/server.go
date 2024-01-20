package chandy_lamport

import (
	"log"
	"strconv"
	"sync"
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
	// TODO: ADD MORE FIELDS HERE

	//process_state is used to store the tokens local to the process and load when required
	process_state                *SyncMap
	snapshot_status              map[int]bool
	inbound_channel_marker_visit *SyncMap
	inbound_channel_record_state *SyncMap
	n_channel                    map[int]*NotifyChannel
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

type NotifyChannel struct {
	notify_channel chan bool
	lock           sync.RWMutex
}

func NewServer(id string, tokens int, sim *Simulator, mapSyn *SyncMap) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		mapSyn,
		make(map[int]bool),
		NewSyncMap(),
		NewSyncMap(),
		make(map[int]*NotifyChannel),
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
	// TODO: IMPLEMENT ME
	//var inbound_channel *Link
	inbound_channel := server.inboundLinks[src]
	//current process is the destination process for the in bound channel which is the server Id of the current server
	current_process := server.Id
	message_type := message

	// if message_type == MarkerMessage {

	// }
	message_data, isMarker := message_type.(MarkerMessage)
	// onrecieving marker we  are checking for whether the process is already recorded if not we are starting the snapshot
	if isMarker {

		// creating unique identifier for marker traversal via inbound channel
		S_Id := message_data.snapshotId
		Source_Id := inbound_channel.src
		Destination_Id := inbound_channel.dest
		// key format:  snapshotId|src->dest   value: true/false
		channel_val := Source_Id + "->" + Destination_Id
		final_marker_visit_val := strconv.Itoa(S_Id) + "|" + channel_val
		server.inbound_channel_marker_visit.Store(final_marker_visit_val, true)
		_, already_snapshotted := server.process_state.Load(S_Id)
		if !already_snapshotted {
			server.snapshot_status[S_Id] = true
			server.StartSnapshot(S_Id)
			//initializing the inbound channel recieved messages as empty as per algorithm (given in the handout)
			server.inbound_channel_record_state.Store(final_marker_visit_val, "")
		}
		//check_in_channel := true
		check_in_channel_count := 0
		for src_val, _ := range server.inboundLinks {
			c_val := src_val + "->" + current_process
			final_marker_val := strconv.Itoa(S_Id) + "|" + c_val
			_, already_visited := server.inbound_channel_marker_visit.Load(final_marker_val)
			if already_visited {
				check_in_channel_count += 1
			}
		}

		if check_in_channel_count == len(server.inboundLinks) {
			server.snapshot_status[S_Id] = false
			server.sim.NotifySnapshotComplete(current_process, S_Id)
		}

	} else {
		// if it is a message/token, we are getting the tokens and updating the count
		message_data_Token, _ := message_type.(TokenMessage)
		message_recieved := message_data_Token.numTokens
		// if the snapshot is not active then tokens will be added as part of the server.
		if len(server.snapshot_status) <= 0 {
			server.Tokens = server.Tokens + message_recieved
		} else {
			// if the snapshot is active then tokens are added as part of the inbound channel buffer
			server.Tokens = server.Tokens + message_recieved
			for id, status := range server.snapshot_status {
				S_Id := id
				channel_val1 := inbound_channel.src + "->" + inbound_channel.dest
				final_marker_visit_val1 := strconv.Itoa(S_Id) + "|" + channel_val1
				_, traversed := server.inbound_channel_marker_visit.Load(final_marker_visit_val1)
				// snapshot is active and inbound channel has not been traversed by the marker
				if status && !traversed {
					data, status := server.inbound_channel_record_state.Load(final_marker_visit_val1)
					if status {
						//if messages already exists we append, used pipe symbol as separator between messages
						data_res, data_ok := data.(string)
						if data_ok {
							append_message := data_res + "|" + strconv.Itoa(message_recieved)
							server.inbound_channel_record_state.Store(final_marker_visit_val1, append_message)
						}

					} else {
						// if it is the first message, we directly set the value
						server.inbound_channel_record_state.Store(final_marker_visit_val1, strconv.Itoa(message_recieved))
					}
				}

			}
		}

	}

}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	var process_tokens int
	process_tokens = server.Tokens
	// Tracking snapshot with respect to the snapshot Id
	server.snapshot_status[snapshotId] = true
	server.process_state.Store(snapshotId, process_tokens)
	server.SendToNeighbors(MarkerMessage{snapshotId})

}
