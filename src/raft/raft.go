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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"labrpc"
)

const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (Lab-RA, Lab-RB).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	votedFor    int

	// volatile state on all servers
	logs        []LogEntry
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// other
	state    int
	leaderId int
	applyCh  chan ApplyMsg

	// for election
	lastHeartbeatTime time.Time
	electionTimeout   time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (Lab-RA).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (Lab-RA, Lab-RB).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (Lab-RA).
	Term        int
	VoteGranted bool
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) becomeCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
}

func (rf *Raft) becomeLeader(term int) {
	rf.mu.Lock()
	rf.state = LEADER
	rf.votedFor = -1
	rf.currentTerm = term
	rf.mu.Unlock()
	fmt.Println("Became leader", rf.me, "term", rf.currentTerm)
	rf.sendAppendEntries()
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	if rf.getLastLogIndex() == -1 {
		return 0
	}
	return rf.logs[rf.getLastLogIndex()].Term
}

func (rf *Raft) isCandidateLogUpToDate(candidateLastIndex int, candidateLastTerm int) bool {
	myLastIndex := rf.getLastLogIndex()
	myLastTerm := rf.getLastLogTerm()

	fmt.Println("isCandidateLogUpToDate", rf.me, "myLastIndex", myLastIndex, "myLastTerm", myLastTerm, "candidateLastIndex", candidateLastIndex, "candidateLastTerm", candidateLastTerm)
	if candidateLastTerm == myLastTerm {
		fmt.Println("Helloooooo")
		return candidateLastIndex >= myLastIndex
	}

	return candidateLastTerm > myLastTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (Lab-RA, Lab-RB).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("Received RequestVote at ", rf.me, " my term: ", rf.currentTerm, "leader term: ", args.Term, "voted", reply.VoteGranted, "for", args.CandidateId)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if rf.state == CANDIDATE && args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}

	fmt.Println("Candidate log ", rf.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm))
	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) &&
		rf.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeartbeatTime = time.Now()
		rf.currentTerm = args.Term
	}

	fmt.Println("RequestVote at ", rf.me, " my term: ", rf.currentTerm, "leader term: ", args.Term, "voted", reply.VoteGranted, "for", args.CandidateId)
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
	fmt.Println("sdend Request Vote ", args.CandidateId, "to server ", server, "term", args.Term)
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

	// Your code here (Lab-RB).

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

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.becomeCandidate()
	fmt.Println("\n\nElection timeout, start election by server ", rf.me, "for term ", rf.currentTerm, rf.lastHeartbeatTime, rf.electionTimeout)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	voteCount := 1

	// reset vote channel
	voteChannel := make(chan RequestVoteReply, len(rf.peers))
	rf.lastHeartbeatTime = time.Now()
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me {
			go func(server int, voteChannel chan RequestVoteReply) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				if ok {
					voteChannel <- reply
				}
			}(server, voteChannel)
		}

		for reply := range voteChannel {
			rf.mu.Lock()
			if rf.state != CANDIDATE {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if reply.VoteGranted {
				voteCount++
				if voteCount > len(rf.peers)/2 {
					rf.becomeLeader(args.Term)
					return
				}
			}
		}
	}
}

type AppendEntriesArgs struct {
	Term       int
	LeaderId   int
	LogEntries []LogEntry
}

type AppendEntriesReply struct {
	Success bool
	Term    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		reply.Success = true
		rf.lastHeartbeatTime = time.Now()
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		fmt.Println("AppendEntries received by ", rf.me, "at term ", rf.currentTerm, " state ", rf.state, rf.lastHeartbeatTime)
	}
}

func (rf *Raft) sendAppendEntries() {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:       rf.currentTerm,
		LeaderId:   rf.me,
		LogEntries: rf.logs,
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		// fmt.Println("sendAppendEntries at", peer)
		reply := AppendEntriesReply{}
		go func(peerId int) {
			if peerId == rf.me {
				return
			}

			fmt.Println("Sent append entries to", peerId, "at", rf.me, "with term", rf.currentTerm, "and state", rf.state, "and lastHeartbeatTime", rf.lastHeartbeatTime)
			rf.peers[peerId].Call("Raft.AppendEntries", &args, &reply)
		}(peer)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (Lab-RA)
		// Check if a leader election should be started.
		// if rf.lastHeartbeatTime.Add(rf.electionTimeout).UnixNano() < time.Now().UnixNano() {
		// 	fmt.Println("Election timeout, start election", rf.me, rf.lastHeartbeatTime, rf.electionTimeout)
		// 	rf.startElection()
		// }
		// electionWait := <-rf.electionTimer.C
		rf.mu.Lock()
		fmt.Println("Ticker:", rf.me, "state: ", rf.state, rf.lastHeartbeatTime, rf.electionTimeout)
		if rf.state == FOLLOWER && rf.lastHeartbeatTime.Add(rf.electionTimeout).UnixNano() < time.Now().UnixNano() {
			rf.mu.Unlock()
			go rf.startElection()
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		rf.mu.Lock()
		if rf.state == CANDIDATE && rf.lastHeartbeatTime.Add(rf.electionTimeout).UnixNano() < time.Now().UnixNano() {
			rf.mu.Unlock()
			rf.becomeFollower(rf.currentTerm)
			rf.mu.Lock()
		}
		rf.mu.Unlock()

		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			fmt.Println("Leader Heartbeat send append entries from ", rf.me, rf.lastHeartbeatTime, rf.electionTimeout)
			rf.sendAppendEntries()
			rf.mu.Lock()
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (Lab-RA, Lab-RB).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	rf.lastHeartbeatTime = time.Now()
	rf.electionTimeout = time.Duration(150+(rand.Int63()%500)) * time.Millisecond
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
