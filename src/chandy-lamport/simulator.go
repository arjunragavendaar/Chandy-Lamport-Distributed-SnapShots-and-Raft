package chandy_lamport

import (
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// Max random delay added to packet delivery
const maxDelay = 5

// Simulator is the entry point to the distributed snapshot application.
//
// It is a discrete time simulator, i.e. events that happen at time t + 1 come
// strictly after events that happen at time t. At each time step, the simulator
// examines messages queued up across all the links in the system and decides
// which ones to deliver to the destination.
//
// The simulator is responsible for starting the snapshot process, inducing servers
// to pass tokens to each other, and collecting the snapshot state after the process
// has terminated.
type Simulator struct {
	time           int
	nextSnapshotId int
	servers        map[string]*Server // key = server ID
	logger         *Logger
	donechannel    *SyncMap
	//n_channel      map[int]*notifyChannel
	// TODO: ADD MORE FIELDS HERE
}

func NewSimulator() *Simulator {
	return &Simulator{
		0,
		0,
		make(map[string]*Server),
		NewLogger(),
		NewSyncMap(),
		//make(map[int]*notifyChannel),
	}
}

// type notifyChannel struct {
// 	notify_channel chan bool
// 	lock           sync.RWMutex
// }

// new_notify_channel:= &notifyChannel{
// 	notify_channel: make(chan bool, len(NewSimulator().servers)),
// 	lock:           sync.RWMutex{},
// }

//var notify_channel = make(map[int]chan bool)

//var n_channel = make(map[int]*notifyChannel)

// Return the receive time of a message after adding a random delay.
// Note: since we only deliver one message to a given server at each time step,
// the message may be received *after* the time step returned in this function.
func (sim *Simulator) GetReceiveTime() int {
	return sim.time + 1 + rand.Intn(5)
}

// Add a server to this simulator with the specified number of starting tokens
func (sim *Simulator) AddServer(id string, tokens int) {
	server := NewServer(id, tokens, sim, NewSyncMap())
	sim.servers[id] = server
}

// Add a unidirectional link between two servers
func (sim *Simulator) AddForwardLink(src string, dest string) {
	server1, ok1 := sim.servers[src]
	server2, ok2 := sim.servers[dest]
	if !ok1 {
		log.Fatalf("Server %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Server %v does not exist\n", dest)
	}
	server1.AddOutboundLink(server2)
}

// Run an event in the system
func (sim *Simulator) InjectEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.servers[event.src]
		src.SendTokens(event.tokens, event.dest)
	case SnapshotEvent:
		sim.StartSnapshot(event.serverId)
	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step, handling all send message events
// that expire at the new time step, if any.
func (sim *Simulator) Tick() {
	sim.time++
	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the servers,
	// we must also iterate through the servers and the links in a deterministic way
	for _, serverId := range getSortedKeys(sim.servers) {
		server := sim.servers[serverId]
		for _, dest := range getSortedKeys(server.outboundLinks) {
			link := server.outboundLinks[dest]
			// Deliver at most one packet per server at each time step to
			// establish total ordering of packet delivery to each server
			if !link.events.Empty() {
				e := link.events.Peek().(SendMessageEvent)
				if e.receiveTime <= sim.time {
					link.events.Pop()
					sim.logger.RecordEvent(
						sim.servers[e.dest],
						ReceivedMessageEvent{e.src, e.dest, e.message})
					sim.servers[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// Start a new snapshot process at the specified server
func (sim *Simulator) StartSnapshot(serverId string) {

	var server_obj *Server
	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++
	// for id, _ := range sim.servers {
	// 	sim.servers[id].n_channel[snapshotId] = &NotifyChannel{
	// 		notify_channel: make(chan bool),
	// 		lock:           sync.RWMutex{},
	// 	}
	// 	//fmt.Printf("inside")
	// }

	sim.donechannel.Store(snapshotId, make(chan bool, len(sim.servers)))
	sim.logger.RecordEvent(sim.servers[serverId], StartSnapshot{serverId, snapshotId})

	// TODO: IMPLEMENT ME

	server_obj = sim.servers[serverId]

	// starting the snapshot process from simulator which inturn calls the StartSnapshot method in server file
	server_obj.StartSnapshot(snapshotId)
}

// Callback for servers to notify the simulator that the snapshot process has
// completed on a particular server
func (sim *Simulator) NotifySnapshotComplete(serverId string, snapshotId int) {
	sim.logger.RecordEvent(sim.servers[serverId], EndSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME

	//notify_channel[snapshotId] <- true

	//n_channel[snapshotId].lock.Lock()
	//sim.n_channel[snapshotId].notify_channel <- true
	//n_channel[snapshotId].lock.Unlock()

	//for the corresponding snapshotID and serverID we are setting the true for the respective channel
	// if _, ok := sim.servers[serverId].n_channel[snapshotId]; ok {
	// 	sim.servers[serverId].n_channel[snapshotId].lock.Lock()
	// 	defer sim.servers[serverId].n_channel[snapshotId].lock.Unlock()

	// 	sim.servers[serverId].n_channel[snapshotId].notify_channel <- true

	resultant_channel, response := sim.donechannel.Load(snapshotId)
	if response {
		done_channel, resp := resultant_channel.(chan bool)
		if resp {
			done_channel <- true
		}

	}

	//}
}

// Collect and merge snapshot state from all the servers.
// This function blocks until the snapshot process has completed on all servers.

func collect_snap(collect chan bool, main_n_channel chan bool) {
	<-main_n_channel
	collect <- true
}
func (sim *Simulator) CollectSnapshot(snapshotId int) *SnapshotState {
	snap := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)}
	// TODO: IMPLEMENT ME
	//<-notify_channel[snapshotId]
	// channel_now, _ := n_channel.Load(snapshotId)
	// parsed_channel, ok := channel_now.(chan bool)
	// if ok {
	// 	<-parsed_channel
	// }

	//<-n_channel[snapshotId].notify_channel
	//collect := make(chan bool, len(sim.servers))
	// for ID, _ := range sim.servers {
	// 	//fmt.Printf("%v", ID)

	// 	if _, ok := sim.servers[ID].n_channel[snapshotId]; ok {
	// 		sim.servers[ID].n_channel[snapshotId].lock.RLock()
	// 		defer sim.servers[ID].n_channel[snapshotId].lock.RUnlock()

	// 		temp := <-sim.servers[ID].n_channel[snapshotId].notify_channel
	// 		fmt.Print(temp)

	// 		// go func(collect chan bool, main_n_channel chan bool) {
	// 		// 	<-main_n_channel
	// 		// 	collect <- true
	// 		// }(collect, sim.servers[ID].n_channel[snapshotId].notify_channel)

	// 	}
	// }

	for i := 0; i < len(sim.servers); i += 1 {

		resultant_channel, response := sim.donechannel.Load(snapshotId)
		if response {
			done_channel, resp := resultant_channel.(chan bool)
			if resp {
				<-done_channel
			}

		}

	}

	// for i := 0; i < len(sim.servers); i += 1 {
	// 	<-collect
	// }

	time.Sleep(1 * time.Millisecond)

	for ID, _ := range sim.servers {
		sim.servers[ID].process_state.lock.RLock()
		defer sim.servers[ID].process_state.lock.RUnlock()
		localstate_tokens, done := sim.servers[ID].process_state.Load(snapshotId)
		if done {
			intval, ok := localstate_tokens.(int)
			if ok {
				snap.tokens[ID] = intval
			}

		}

		for snap_shot_id, incoming_messages := range sim.servers[ID].inbound_channel_record_state.internalMap {
			value, ok := snap_shot_id.(string)
			value2, ok2 := incoming_messages.(string)
			if ok && ok2 {
				key_entitites := process_snapshot_key(value)
				conv_snap_id := strconv.Itoa(snapshotId)
				processed_snap_id := key_entitites[0]
				processed_src := key_entitites[1]
				processed_dst := key_entitites[2]
				if len(value2) > 0 && processed_snap_id == conv_snap_id {
					//extracting each message from concatenated message list
					message_list := strings.Split(value2, "|")
					for i := 0; i < len(message_list); i += 1 {
						// create snap message structure
						conv_msg, conv_ok := strconv.Atoi(message_list[i])
						if conv_ok == nil {
							snapargs := SnapshotMessage{processed_src, processed_dst, TokenMessage{conv_msg}}
							snap.messages = append(snap.messages, &snapargs)
						}

					}

				}
			}

		}

	}

	return &snap
}

func process_snapshot_key(snap_shot_id string) [3]string {
	result1 := strings.Split(snap_shot_id, "|")
	snap_id_value := result1[0]
	result2 := strings.Split(result1[1], "->")
	src_value := result2[0]
	dest_value := result2[1]
	var final_result [3]string
	final_result[0] = snap_id_value
	final_result[1] = src_value
	final_result[2] = dest_value
	return final_result
}
