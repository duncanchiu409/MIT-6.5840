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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor    int
	logs        []int

	// volatile state
	commitIndex   int
	lastApplied   int
	currentRole   int
	currentLeader int

	// volatile state on leaders
	voteReceived   []int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	// nextIndex    []int
	// matchIndex   []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.currentRole == 2
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	VoterId     int
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	myLogTerm := -1
	termOk := (args.Term > rf.currentTerm) || (args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId))
	logOk := (args.LastLogTerm > myLogTerm) || (args.LastLogTerm == myLogTerm && args.LastLogIndex >= -1)
	if termOk && logOk {
		rf.currentTerm = args.Term
		rf.currentRole = 0
		rf.votedFor = args.CandidateId
		reply.VoterId = rf.me
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.heartbeatTimer.Stop()
		DPrintf("server %v vote for %v is %v ", rf.me, args.CandidateId, reply.VoteGranted)
	} else {
		reply.VoterId = rf.me
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("server %v rejected vote for %v is %v ", rf.me, args.CandidateId, reply.VoteGranted)
	}
	rf.electionTimer.Reset(time.Duration(350+(rand.Int63()%300)) * time.Millisecond)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	NodeId  int
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	logOk := (len(rf.logs)-1 >= args.PrevLogIndex)
	if logOk && (args.PrevLogIndex >= 0) {
		logOk = (args.PrevLogTerm == -1)
	}
	if args.Term == rf.currentTerm && logOk {
		DPrintf("server %v accept appendEntries from %v", rf.me, args.LeaderId)
		rf.currentRole = 0
		rf.currentLeader = args.LeaderId
		reply.NodeId = rf.me
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.heartbeatTimer.Stop()
	} else {
		DPrintf("server %v refuse appendEntries from %v", rf.me, args.LeaderId)
		reply.NodeId = rf.me
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	rf.electionTimer.Reset(time.Duration(350+(rand.Int63()%300)) * time.Millisecond)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	voteRequestReplies := make(chan RequestVoteReply)
	appendEntriesReplies := make(chan AppendEntriesReply)
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		// Receiving Vote
		case reply := <-voteRequestReplies:
			rf.mu.Lock()
			if rf.currentRole == 1 && reply.Term == rf.currentTerm && reply.VoteGranted {
				if !Contains(rf.voteReceived, reply.VoterId) {
					rf.voteReceived = append(rf.voteReceived, reply.VoterId)
				}
				voteReceived := len(rf.voteReceived)
				DPrintf("%v", rf.voteReceived)
				if voteReceived >= (len(rf.peers)+1)/2 {
					DPrintf("server %v won the election for term %v with %v votes", rf.me, rf.currentTerm, voteReceived)
					rf.currentRole = 2
					rf.currentLeader = rf.me
					rf.electionTimer.Stop()
					rf.heartbeatTimer.Reset(time.Duration(100) * time.Millisecond)
				}
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.currentRole = 0
				rf.votedFor = -1
				rf.electionTimer.Stop()
				rf.heartbeatTimer.Stop()
			}
			rf.mu.Unlock()
		// Receiving Append Entries
		case reply := <-appendEntriesReplies:
			rf.mu.Lock()
			if reply.Term == rf.currentTerm && rf.currentRole == 2 {
				if reply.Success {

				} else {

				}
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.currentRole = 0
				rf.votedFor = -1
				rf.heartbeatTimer.Stop()
			}
			rf.mu.Unlock()
		// Election Timeout
		case <-rf.electionTimer.C:
			DPrintf("server %v election timer fired", rf.me)
			rf.mu.Lock()
			rf.electionTimer.Reset(time.Duration(350+(rand.Int63()%300)) * time.Millisecond)
			rf.currentTerm = rf.currentTerm + 1
			rf.currentRole = 1
			rf.votedFor = rf.me
			rf.voteReceived = []int{rf.me}
			args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastApplied, -1}
			rf.mu.Unlock()

			DPrintf("server %v starting election", rf.me)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(voterId int) {
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(voterId, &args, &reply)
						if ok {
							voteRequestReplies <- reply
						}
					}(i)
				}
			}
		// Periodically Append
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			DPrintf("server %v in state %v heartbeat ticker fired", rf.me, rf.currentRole)
			state := rf.currentRole

			if state == 2 {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.currentLeader,
					PrevLogIndex: -1,
					PrevLogTerm:  -1,
					Entries:      []int{},
					LeaderCommit: rf.commitIndex,
				}
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go func(nodeId int) {
							DPrintf("server %v send appendEntries to peer %v", rf.me, nodeId)
							reply := AppendEntriesReply{}
							ok := rf.sendAppendEntries(nodeId, &args, &reply)
							if ok {
								appendEntriesReplies <- reply
							}

						}(i)
					}
				}
			}
			rf.heartbeatTimer.Reset(time.Duration(100) * time.Millisecond)
			rf.mu.Unlock()
		}
	}
	// close(voteRequestReplies)
	// close(appendEntriesReplies)
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.currentRole = 0
	rf.currentLeader = -1
	rf.electionTimer = time.NewTimer(time.Duration(50+(rand.Int63()%300)) * time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(time.Duration(100) * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
