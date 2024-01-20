package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
// added attributes as given in the research paper titled "In Search of an Understandable Consensus Algorithm"
type Raft struct {
	mu             sync.Mutex
	peers          []*labrpc.ClientEnd
	persister      *Persister
	me             int // index into peers[]
	currRole       int
	currLeader     int
	votes_received []bool
	currentTerm    int
	votedFor       int

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

const role_leader int = 1
const role_candidate int = 2
const role_follower int = 3
const initial_vote = -999
const initial_leader = -1

var timer_start time.Duration = time.Duration(rand.Intn(150)+150) * time.Millisecond

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	if rf.currRole == 1 {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
}

// example RequestVote RPC arguments structure.
// added attributes as given in the research paper titled "In Search of an Understandable Consensus Algorithm"
type RequestVoteArgs struct {
	// Your data here.
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// added attributes as given in the research paper titled "In Search of an Understandable Consensus Algorithm"
type RequestVoteReply struct {
	// Your data here.
	VoteGranted bool
	Term        int
}

type AppendEntriesRPCRequest struct {
	Term     int
	LeaderId int
}
type AppendEntriesRPCReply struct {
	Term   int
	Status bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	//checking if the node that is receiving the vote has larger term or not, if not updating the current term with the larger term.
	//request vote reciever implementation given in the research paper
	rf.mu.Lock()
	defer rf.mu.Unlock()
	sentnode_term := args.Term
	receivingnode_term := rf.currentTerm
	if sentnode_term > receivingnode_term {
		rf.currentTerm = sentnode_term
		rf.currRole = role_follower
		rf.votedFor = initial_vote
		reply.Term = sentnode_term
		reply.VoteGranted = true
		rf.persist()
	} else {
		reply.Term = receivingnode_term
		reply.VoteGranted = false
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.currRole == role_candidate && reply.VoteGranted {
		rf.votes_received = append(rf.votes_received, true)
		majority := (len(rf.peers) / 2) + 1
		if len(rf.votes_received) >= majority {
			rf.currRole = role_leader
			rf.currLeader = rf.me
			rf.votedFor = initial_vote
			rf.persist()
		}
	} else if ok && reply.Term > rf.currentTerm && !reply.VoteGranted {
		rf.currRole = role_follower
		rf.currLeader = initial_leader
		rf.votedFor = initial_vote
		rf.persist()
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func (rf *Raft) AppendEntries(args *AppendEntriesRPCRequest, reply *AppendEntriesRPCReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	receiving_node_term := rf.currentTerm
	sent_node_term := args.Term

	if sent_node_term > receiving_node_term {
		reply.Term = sent_node_term
		reply.Status = true
		rf.currentTerm = sent_node_term
		rf.currRole = role_follower
		//leader updation
		rf.currLeader = initial_leader
		rf.votedFor = initial_vote
		rf.persist()
	} else if receiving_node_term > sent_node_term {
		reply.Status = false
		reply.Term = receiving_node_term
		return
	}
}
func askvote(node int, rf *Raft) {
	current_node := rf.me
	prepare_request_args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: current_node,
	}
	prepare_reply_args := RequestVoteReply{}
	rf.sendRequestVote(node, prepare_request_args, &prepare_reply_args)
}
func heartbeat(node int, rf *Raft) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ae_rpc_req := AppendEntriesRPCRequest{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	ae_rpc_reply := AppendEntriesRPCReply{}
	resp := rf.peers[node].Call("Raft.AppendEntries", &ae_rpc_req, &ae_rpc_reply)
	if resp && ae_rpc_reply.Term > rf.currentTerm {
		rf.currentTerm = ae_rpc_reply.Term
		rf.currRole = role_follower
		rf.currLeader = initial_leader
		rf.votedFor = initial_vote
		rf.persist()
		return
	}

}
func begin_process(rf *Raft) {

	for {
		get_current_node_role := rf.currRole
		available_nodes := rf.peers
		current_active_node := rf.me
		if get_current_node_role == role_leader {
			for node := range available_nodes {
				if node != current_active_node {
					go heartbeat(node, rf)
				}
			}

		} else if get_current_node_role == role_candidate {
			for node := range available_nodes {
				if node != current_active_node {
					go askvote(node, rf)
				}
			}
			<-time.After(time.Duration((rand.Intn(150))+150) * time.Millisecond)
			rf.mu.Lock()
			rf.votedFor = current_active_node
			rf.votes_received = rf.votes_received[:0]
			rf.votes_received = append(rf.votes_received, true)
			rf.currRole = role_candidate
			rf.currentTerm = rf.currentTerm + 1
			rf.mu.Unlock()
			rf.persist()

		} else if get_current_node_role == role_follower {

			<-time.After(time.Duration((rand.Intn(150))+150) * time.Millisecond)
			rf.mu.Lock()
			rf.votedFor = current_active_node
			rf.votes_received = rf.votes_received[:0]
			rf.votes_received = append(rf.votes_received, true)
			rf.currRole = role_candidate
			rf.currentTerm = rf.currentTerm + 1
			rf.mu.Unlock()
			rf.persist()

		}

	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.currLeader = initial_leader
	rf.currRole = role_follower
	rf.votedFor = initial_vote

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	// starting go routines as specified
	go begin_process(rf)
	return rf
}
