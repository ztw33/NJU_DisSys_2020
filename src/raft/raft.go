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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const MinTimeout = 150
const MaxTimeout = 300
const HeartbeatTimeout = 225


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

type State int
const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

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
	currentTerm	int
	votedFor	int
	logs		[]LogEntry

	// Volatile state on all servers
	commitIndex	int
	lastApplied	int

	// Volatile state on leaders
	nextIndex	[]int
	matchIndex	[]int

	// Defined by myself
	state		State
	timer		*time.Timer
	voteCount	int // for leader
	resetTimer	chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
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
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term	int
	VoteGranted	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO: reset timer(done but not sure)
	if rf.state == FOLLOWER {
		rf.timer.Reset(getRandomTimeout())
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[RequestVote] server[%d] rejects voting to server[%d](candidate). Candidate term < server[%d] currentTerm", rf.me, args.CandidateId, rf.me)
	} else {
		lastLogIndex := len(rf.logs) - 1
		var lastLogTerm int
		if lastLogIndex < 0 {
			lastLogTerm = -1
		} else {
			lastLogTerm = rf.logs[lastLogIndex].Term
		}
		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			DPrintf("[RequestVote] server[%d] votes to server[%d](candidate).", rf.me, args.CandidateId)
			// TODO: 什么时候更新[done:rf的term]以及log？
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			if !(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
				DPrintf("[RequestVote] server[%d] rejects voting to server[%d](candidate). votedFor is not null or candidateId", rf.me, args.CandidateId)
			}
			if !(args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= lastLogIndex) {
				DPrintf("[RequestVote] server[%d] rejects voting to server[%d](candidate). Candidate's log is not as up-to-date as receiver's log.", rf.me, args.CandidateId)
			}
		}
		if args.Term > rf.currentTerm { // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			DPrintf("[RequestVote] server[%d] updates current term %d to server[%d](candidate)'s term %d.", rf.me, rf.currentTerm, args.CandidateId, args.Term)
			rf.currentTerm = args.Term
			rf.convert2Follower()
		}
	}
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
	if ok {
		DPrintf("[sendRequestVote] server[%d](candidate) receives reply from server[%d]. Result is %t", rf.me, server, reply.VoteGranted)
	} else {
		DPrintf("[sendRequestVote] server[%d](candidate) cannot receive from server[%d].", rf.me, server)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.state == CANDIDATE {
		if rf.currentTerm < reply.Term { // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			rf.currentTerm = reply.Term
			rf.convert2Follower()
		} else {
			if reply.VoteGranted {
				rf.voteCount++
				if rf.voteCount > len(rf.peers)/2 {
					DPrintf("[sendRequestVote] server[%d](candidate) receives from majority of servers. Become leader.", rf.me)
					rf.state = LEADER
					go rf.loopAsLeader()
				}
			}
		}
	}
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
	DPrintf("[Make] create Raft server. me:%d", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.timer = time.NewTimer(getRandomTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.loopAsFollower()
	return rf
}


// My code
func getRandomTimeout() time.Duration {
	return time.Duration(rand.Intn(MaxTimeout-MinTimeout)+MinTimeout) * time.Millisecond
}

func (rf *Raft) loopAsFollower()  {
	for rf.state == FOLLOWER {
		DPrintf("follower me: %d", rf.me)
		select {
		case reset := <-rf.resetTimer:
			if reset {
				rf.timer.Reset(getRandomTimeout())
			}
		case <-rf.timer.C: // election timeout, become candidate
			rf.state = CANDIDATE
			go rf.loopAsCandidate()
		}

	}
}

func (rf *Raft) loopAsCandidate() {
	for rf.state == CANDIDATE {
		rf.mu.Lock()
		rf.currentTerm++ // Increment currentTerm
		rf.votedFor = rf.me // Vote for self
		rf.voteCount = 1
		DPrintf("[loopAsCandidate] server[%d] becomes candidate. Current term: %d", rf.me, rf.currentTerm)
		rf.timer.Reset(getRandomTimeout()) // Reset election timer
		// Send RequestVote RPCs to all other servers
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				// TODO: lastLogIndex & lastLogTerm
				args.LastLogIndex = -1
				args.LastLogTerm = -1
				reply := &RequestVoteReply{}
				DPrintf("[loopAsCandidate] server[%d](candidate) sends request vote to server[%d].", rf.me, i)
				go rf.sendRequestVote(i, args, reply)
			}
		}
		rf.mu.Unlock()
		select {
		case <-rf.timer.C: // election timeout, start new election

		}


	}
}

func (rf *Raft) loopAsLeader() {
	for rf.state == LEADER {
		// Send AppendEntries RPCs to all other server
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderID = rf.me
				args.LeaderCommit = rf.commitIndex
				// TODO: Entries, PrevLogIndex, PrevLogTerm
				args.PrevLogIndex = -1
				args.PrevLogTerm = -1
				reply := &AppendEntriesReply{}
				DPrintf("[loopAsLeader] server[%d](leader) sends append entries to server[%d].", rf.me, i)
				go rf.sendAppendEntries(i, args, reply)
			}
		}
		rf.timer.Reset(time.Duration(HeartbeatTimeout) * time.Millisecond)
		select {
		case <-rf.timer.C: // heartbeat timeout, send append entries

		}
	}
}

func (rf *Raft) convert2Follower()  {
	if rf.state != FOLLOWER {
		DPrintf("[convert2Follower] server[%d] converts to follower.", rf.me)
		rf.state = FOLLOWER
		rf.timer.Reset(getRandomTimeout())
		rf.voteCount = 0
		rf.votedFor = -1
		go rf.loopAsFollower()
	}
}

type AppendEntriesArgs struct {
	Term			int
	LeaderID		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term		int
	Success		bool
}

type LogEntry struct {
	Term		int
	Command		interface{}
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("[AppendEntries]")
	if args.Term < rf.currentTerm { // Reply false if term < currentTerm
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// TODO: log replication
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.resetTimer <- true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		DPrintf("[sendAppendEntries] server[%d](leader) receives reply from server[%d]. Result is %t", rf.me, server, reply.Success)
	} else {
		DPrintf("[sendAppendEntries] server[%d](leader) cannot receive from server[%d].", rf.me, server)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.state == LEADER {
		if reply.Success {
			// TODO
		} else {
			if rf.currentTerm < reply.Term { // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
				rf.currentTerm = reply.Term
				rf.convert2Follower()
			}
		}
	}
	return ok
}