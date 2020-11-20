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
	"sync"
	"labrpc"
	"bytes"
	"encoding/gob"
	"time"
	"math/rand"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing the logEntries in Raft.
//
type logEntries struct {
	term     int
	logIndex int
	Command  interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// State.
	serverState string

	// Persistent state on all servers:
	// Updated on stable storage before responding to RPCs.
	currentTerm int
	votedFor    int
	log         []*logEntries

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	// Reinitialized after election.
	nextIndex   []int
	matchIndex  []int

	// Auxiliary:
	electionTimeout int
	leaderTimeout   int
	votesCount      int
}

// return currentTerm and whether this server
// believes it is the Leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	
	// Your code here.
	
	// Get the current term from persistent state.
	term = rf.currentTerm
	// If the server believes it is the Leader return true.
	if rf.serverState == "Leader" {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	
	// Save the persistent state to stable storage in Raft struct.
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.

	// Restore the persistent state previously stored into stable storage.
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	candidateTerm int
	candidateID  int
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	candidateCurrentTerm int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	
	//	Reject the vote for a candidate if it is in a lower term.
	if args.candidateTerm < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		//	If the term of a candidate server or leader server is outdated, it becomes Follower.
		rf.serverState = "Follower"
		rf.currentTerm = args.candidateTerm
		// Reset the election timeout with an arbitrary length of time: random time between 150 and 300.
		rf.electionTimeout = 150 + rand.Intn(150) // Starts Election timer
		reply.VoteGranted = true
	}
	
	// Update the term of the candidate with current term.
	reply.candidateCurrentTerm = rf.currentTerm
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// Invoked by leader to replicate log entries.
// Also used as heartbeat.
//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	CurrentTerm       int
	leaderId          int
	prevLogIndex      int
	prevLogTerm       int
	entries           []*logEntries
	leaderCommitIndex int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	currentLeaderTerm int
	Success bool
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Leader use AppendEntries RPC for log replication. Also, AppendEntries RPC is used to send hearbeats.
	if rf.currentTerm <= args.CurrentTerm {
		// If the term of a Leader server is outdated, it becomes Follower.
		rf.serverState = "Follower"
		rf.currentTerm = args.CurrentTerm
		// Reset the election timeout with an arbitrary length of time: random time between 150 and 300.
		rf.electionTimeout = 150 + rand.Intn(150)
	}
	
	// Update the term of the server with current term.
	reply.currentLeaderTerm = rf.currentTerm
}

//
// Send AppendEntries RPC.
//
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// Decided not to implement code for this method.
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.serverState = "Follower"
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]*logEntries, 0)
	rf.log = append(rf.log, &logEntries{0, 0, nil})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Main functionality for Raft leader election.
	go rf.startElection()

	return rf
}

//
// startElection should run in a separate goroutine. It continually checks whether no heartbeats have been received within the election timeout window.
// On receiving a heartbeat, it resets the election timeout. If none received, promotes to candidate state. If already a leader, sends heartbeats.
// Election process will be held if the server is not leader and no heartbeat has been received.
//
func (rf *Raft) startElection() {
	// Reset the election timeout with an arbitrary length of time: random time between 150 and 300.
	rf.electionTimeout = 150 + rand.Intn(150)
	// Hold the millisecond accuracy for the timeouts.
	oneMillisecond := 1 * time.Millisecond
	
	for {
		switch rf.serverState {
			case "Follower":
				// Each server starts as Follower.
				time.Sleep(oneMillisecond)
				rf.electionTimeout--
				if rf.electionTimeout < 0 {
					// No heartbeats were received, promote to Candidate and start election.
					rf.serverState = "Candidate"
				}
			case "Candidate":
				// The server is a Candidate (Leader unavailable). Used to elect a new Leader.
				// No more heartbeats were received, so the election timeout expired.
				if rf.electionTimeout < 0 {
					// Reset the election timeout with an arbitrary length of time: random time between 150 and 300.
					rf.electionTimeout = 150 + rand.Intn(150)
					// Start the term (arbitrary period of time on the server for which a new leader needs to be elected).
					rf.currentTerm++
					// Candidate votes for itself
					rf.votesCount = 1
					// Iterate through all peers to send RPCs for requesting votes.
					for index := range rf.peers {
						if index != rf.me {
							// Invoke RequestVote RPC to gather votes from all other servers.
							// Creates goroutine to avoid blocking the loop while waiting for reply.
							go rf.sendRequestVoteMsgs(index)
						}
					}
				} else {
					// Heartbeats still received.
					if rf.votesCount > int(len(rf.peers)/2) {
						// The server becomes Leader as it received votes from majority of servers.
						rf.serverState = "Leader"
						// Reset timeout of Leader to send heartbeats immediately.
						rf.leaderTimeout = 0
					} else {
						// Heartbeats being received within election timeout window.
						// There was a split vote, then wait until election timeout window ends in order to start a new election.
						time.Sleep(oneMillisecond)
						rf.electionTimeout--
					}
				}
			case "Leader":
				// This server is the Leader. Handles all client interactions and log replication. At most 1 viable Leader at a time.
				// The Leader should send hearbeats within a specific heartbeat timeout window.
				if rf.leaderTimeout < 0 {
					// The timeout for the Leader to send heartbeat should be less than the election timeout, in order to avoid an stale term.
					rf.leaderTimeout = 100
					// Iterate through all peers to check for a Leader.
					for index := range rf.peers {
						if index != rf.me {
							// If already a Leader, send heartbeats to the Followers. Also used by Leader for log replication.
							// Call goroutine to continue the loop in parallel.
							go rf.sendAppendEntriesMsgs(index)
						}
					}
				} else {
					rf.leaderTimeout--
					time.Sleep(oneMillisecond)
				}
			default:
				// Only above three states are recognized in Raft.
				panic("Raft only recognizes Follower, Candidate and Leader as valid states.")
		}
	}
}

//
// Used by Candidate to request votes to other peers.
//
func (rf *Raft) sendRequestVoteMsgs(index int) {
	// Create objects of RequestVoteReply type to be used in RPCs.
	replies := RequestVoteReply{-1, false}
	// Send Request Vote RPC.
	rf.sendRequestVote(index, RequestVoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].logIndex, rf.log[len(rf.log)-1].term}, &replies)
	if replies.VoteGranted == true {
		// Vote was granted, increment general counter of votes.
		rf.votesCount++
	} else {
		// Vote was not granted: replies.VoteGranted = false.
		rf.currentTerm = replies.candidateCurrentTerm
		// Becomes Follower as vote was not granted.
		rf.serverState = "Follower"
	}
}

//
// Used by Leader to replicate log entries. Also used to send hearbeats.
//
func (rf *Raft) sendAppendEntriesMsgs(index int) {
	// Create objects of AppendEntriesReply type to be used in RPCs.
	replies := AppendEntriesReply{-1, false}
	// Send Append Entries RPC.
	rf.sendAppendEntries(index, AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, nil, 0}, &replies)
	if replies.currentLeaderTerm > rf.currentTerm {
		// Discovered a server with higher term.
		rf.currentTerm = replies.currentLeaderTerm
		// The term was not up to date, becomes Follower.
		rf.serverState = "Follower"
		// Reset the election timeout with an arbitrary length of time: random time between 150 and 300.
		rf.electionTimeout = 150 + rand.Intn(150)
	}
}
