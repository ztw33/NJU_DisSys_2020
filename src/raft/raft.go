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
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const MinTimeout = 350
const MaxTimeout = 500
const HeartbeatTimeout = 80

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
	state        State
	timer        *time.Timer
	voteCount    int       // for leader
	resetTimerCh chan bool // for follower
	applyCh      chan ApplyMsg
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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
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
	Term			int
	VoteGranted		bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm { // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		DPrintf("[RequestVote] server[%d] updates current term %d to server[%d](candidate)'s term %d.", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.convert2Follower()
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[RequestVote] server[%d] rejects voting to server[%d](candidate). Candidate term < server[%d] currentTerm", rf.me, args.CandidateId, rf.me)
	} else {
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term
		// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && ((args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			DPrintf("[RequestVote] server[%d] votes to server[%d](candidate).", rf.me, args.CandidateId)
			if rf.state == FOLLOWER { // NOTICE: reset timer only for follower
				rf.resetTimerCh <- true // NOTICE: reset timer only when voting granted
			}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.state == CANDIDATE {
		DPrintf("[sendRequestVote] server[%d](candidate) receives reply from server[%d]. Result is %t", rf.me, server, reply.VoteGranted)
		if rf.currentTerm < reply.Term { // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			rf.convert2Follower()
		} else {
			if reply.VoteGranted && reply.Term == rf.currentTerm { // NOTICE: 增加了应该是当前term的判断
				rf.voteCount++
				if rf.voteCount > len(rf.peers)/2 {
					DPrintf("[sendRequestVote] server[%d](candidate) receives votes from majority of servers.", rf.me)
					rf.convert2Leader()
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == LEADER
	term = rf.currentTerm

	if isLeader {
		rf.logs = append(rf.logs, LogEntry{
			Term:    term,
			Command: command,
		})
		rf.persist()
		//rf.nextIndex[rf.me] = len(rf.logs)
		//rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
		index = len(rf.logs) - 1
		DPrintf("[Start] start an agreement. current leader: server[%d], logs length: %d, logs: %v, index = %d, term = %d", rf.me, len(rf.logs), rf.logs, index, term)
	}

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
	DPrintf("[Make] create Raft server[%d].", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.timer = time.NewTimer(getRandomTimeout())
	rf.resetTimerCh = make(chan  bool)
	rf.commitIndex = 0
	rf.voteCount = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.logs = append(rf.logs, LogEntry{-1, nil}) // NOTICE: insert an empty log!

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
		DPrintf("follower loop: %d, currentTerm: %d, commitIndex: %d\n", rf.me, rf.currentTerm, rf.commitIndex)
		select {
		case reset := <-rf.resetTimerCh:
			if reset {
				DPrintf("[loopAsFollower] server[%d] resets timer.", rf.me)
				rf.timer = time.NewTimer(getRandomTimeout())
			}
		case <-rf.timer.C: // election timeout, become candidate
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.resetTimerCh = make(chan bool) // TODO: not sure
			rf.mu.Unlock()
			go rf.loopAsCandidate()
		}
	}
}

func (rf *Raft) loopAsCandidate() {
	for rf.state == CANDIDATE {
		//fmt.Printf("candidate: %d, commitIndex: %d\n", rf.me, rf.commitIndex)
		rf.mu.Lock()
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		rf.currentTerm++ // Increment currentTerm
		rf.votedFor = rf.me // Vote for self
		rf.voteCount = 1
		rf.persist()
		DPrintf("[loopAsCandidate] server[%d] becomes candidate. Current term: %d", rf.me, rf.currentTerm)
		rf.timer = time.NewTimer(getRandomTimeout()) // Reset election timer
		// Send RequestVote RPCs to all other servers
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = len(rf.logs) - 1
				args.LastLogTerm = rf.logs[args.LastLogIndex].Term
				reply := &RequestVoteReply{}
				DPrintf("[loopAsCandidate] server[%d](candidate) sends request vote to server[%d].", rf.me, i)
				go rf.sendRequestVote(i, args, reply)
			}
		}
		rf.mu.Unlock()
		if rf.state == CANDIDATE {
			select {
			case <-rf.timer.C: // election timeout, start new election
				if rf.state == CANDIDATE {
					DPrintf("[loopAsCandidate] election timeout. Server[%d](candidate) starts new election.", rf.me)
				}
			}
		}
	}
}

func (rf *Raft) loopAsLeader() {
	rf.mu.Lock()
	if rf.state == LEADER {
		// Notice: Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderID = rf.me
				args.LeaderCommit = rf.commitIndex
				args.Entries = make([]LogEntry, 0)
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				reply := &AppendEntriesReply{}
				DPrintf("[loopAsLeader] server[%d](leader) sends initial empty AppendEntries RPCs (heartbeat) to server[%d]. Leader term: %d. args: %v", rf.me, i, rf.currentTerm, *args)
				go rf.sendAppendEntries(i, args, reply)
			}
		}
		rf.timer = time.NewTimer(time.Duration(HeartbeatTimeout) * time.Millisecond)
		DPrintf("[loopAsLeader] server[%d](leader) resets timer.", rf.me)
	}
	rf.mu.Unlock()
	if rf.state == LEADER {
		select {
		case <-rf.timer.C: // heartbeat timeout
			if rf.state == LEADER {
				DPrintf("[loopAsLeader] server[%d](leader) heartbeat timeout.", rf.me)
			}
		}
	}

	for rf.state == LEADER {
		//fmt.Printf("leader: %d, commitIndex: %d, logs length: %d\nnextIndex: %v, matchIndex: %v\n", rf.me, rf.commitIndex, len(rf.logs), rf.nextIndex, rf.matchIndex)
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		// Send AppendEntries RPCs to all other server
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderID = rf.me
				args.LeaderCommit = rf.commitIndex
				args.Entries = rf.logs[rf.nextIndex[i]:]
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				reply := &AppendEntriesReply{}
				DPrintf("[loopAsLeader] server[%d](leader) sends append entries to server[%d]. Leader term: %d. args: %v", rf.me, i, rf.currentTerm, *args)
				go rf.sendAppendEntries(i, args, reply)
			}
		}

		if rf.state == LEADER {
			rf.timer = time.NewTimer(time.Duration(HeartbeatTimeout) * time.Millisecond)
			DPrintf("[loopAsLeader] server[%d](leader) resets timer.", rf.me)
		}
		rf.mu.Unlock()
		if rf.state == LEADER {
			select {
			case <-rf.timer.C: // heartbeat timeout, send append entries
				if rf.state == LEADER {
					DPrintf("[loopAsLeader] server[%d](leader) heartbeat timeout.", rf.me)
				}
			}
		}
	}
}

func (rf *Raft) convert2Follower()  {
	if rf.state != FOLLOWER {
		DPrintf("[convert2Follower] server[%d] converts to follower.", rf.me)
		rf.state = FOLLOWER
		rf.timer = time.NewTimer(getRandomTimeout())
		rf.voteCount = 0
		rf.votedFor = -1
		rf.resetTimerCh = make(chan bool)
		rf.persist()
		go rf.loopAsFollower()
	}
}

func (rf *Raft) convert2Leader() {
	if rf.state != LEADER {
		DPrintf("[convert2Leader] server[%d] converts to leader.", rf.me)
		rf.state = LEADER
		// Volatile state on leaders. Reinitialized after election.
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = 0
		}
		go rf.loopAsLeader()
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
	NextIndex	int // lab3: optimize
}

type LogEntry struct {
	Term		int
	Command		interface{}
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 全部转成follower之后再做处理
	if rf.state == FOLLOWER {
		if args.Term > rf.currentTerm {
			DPrintf("[AppendEntries] server[%d](follower) updates current term %d to server[%d](leader)'s term %d.", rf.me, rf.currentTerm, args.LeaderID, args.Term)
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
		}
	} else if rf.state == CANDIDATE {
		if args.Term >= rf.currentTerm {
			DPrintf("[AppendEntries] server[%d](candidate) updates current term %d to server[%d](leader)'s term %d.", rf.me, rf.currentTerm, args.LeaderID, args.Term)
			rf.currentTerm = args.Term
			rf.convert2Follower()
		}
	} else {
		if args.Term > rf.currentTerm {
			DPrintf("[AppendEntries] server[%d](old leader) updates current term %d to server[%d](leader)'s term %d.", rf.me, rf.currentTerm, args.LeaderID, args.Term)
			rf.currentTerm = args.Term
			rf.convert2Follower()
		}
	}

	if args.Term < rf.currentTerm { // Reply false if term < currentTerm
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = args.PrevLogIndex + 1
		DPrintf("[AppendEntries] server[%d] replies fail append entries to server[%d](leader). Leader's term < currentTerm", rf.me, args.LeaderID)
		return
	}
	//if args.Term > rf.currentTerm { // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	//	DPrintf("[AppendEntries] server[%d] updates current term %d to server[%d](leader)'s term %d.", rf.me, rf.currentTerm, args.LeaderID, args.Term)
	//	rf.currentTerm = args.Term
	//	rf.votedFor = -1 // 遗漏
	//	rf.persist()
	//	rf.convert2Follower()
	//}
	//// for candidate, if AppendEntries RPC received from new leader, convert to follower
	//if rf.state == CANDIDATE && args.Term == rf.currentTerm { // NOTICE: "&& args.Term == rf.currentTerm"
	//	rf.convert2Follower()
	//}
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Term = rf.currentTerm
		reply.Success = false
		if rf.state == FOLLOWER { // NOTICE: reset timer only for follower
			rf.resetTimerCh <- true // NOTICE
		}
		// lab3: optimize
		if args.PrevLogIndex >= len(rf.logs) {
			reply.NextIndex = len(rf.logs)
		} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			conflictTerm := rf.logs[args.PrevLogIndex].Term
			firstIndex := args.PrevLogIndex
			for firstIndex > 0 && rf.logs[firstIndex].Term == conflictTerm {
				firstIndex--
			}
			reply.NextIndex = firstIndex + 1
		}
		DPrintf("[AppendEntries] server[%d] replies fail append entries to server[%d](leader). Terms mismatch.", rf.me, args.LeaderID)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	if args.PrevLogIndex + 1 < len(rf.logs) {
		DPrintf("[AppendEntries] server[%d]'s entry conflicts with a new one. previous logs: %v, current logs: %v", rf.me, rf.logs, rf.logs[:args.PrevLogIndex + 1])
		rf.logs = rf.logs[:args.PrevLogIndex + 1]
	}
	// Append any new entries not already in the log
	rf.logs = append(rf.logs, args.Entries...)
	rf.persist()
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.logs) - 1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.logs) - 1
		}
		DPrintf("[AppendEntries] server[%d] set commitIndex to %d.", rf.me, rf.commitIndex)
		rf.applyMsg()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.NextIndex = len(rf.logs)
	if rf.state == FOLLOWER { // NOTICE: reset timer only for follower
		rf.resetTimerCh <- true
	}
	DPrintf("[AppendEntries] server[%d] replies success append entries to server[%d](leader). logs length: %d, logs: %v", rf.me, args.LeaderID, len(rf.logs), rf.logs)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		DPrintf("[sendAppendEntries] server[%d](leader) receives reply from server[%d]. Result is %t", rf.me, server, reply.Success)
		if rf.state == LEADER {
			if reply.Success && reply.Term == rf.currentTerm { // NOTICE: 增加了应该是当前term的判断
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = rf.nextIndex[server] - 1

				// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and logs[N].term == currentTerm: set commitIndex = N
				for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
					if rf.logs[N].Term == rf.currentTerm { // Figure 8
						count := 1
						for i := 0; i < len(rf.peers); i++ {
							if i != rf.me && rf.matchIndex[i] >= N {
								count++
							}
						}
						if count > len(rf.peers) / 2 {
							DPrintf("[sendAppendEntries] server[%d](leader) sets commitIndex from %d to %d.", rf.me, rf.commitIndex, N)
							rf.commitIndex = N
							break
						}
					}
				}
				rf.applyMsg()
			} else {
				if rf.currentTerm < reply.Term { // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
					DPrintf("[sendAppendEntries] server[%d](old leader) updates current term %d to reply term %d from server[%d].", rf.me, rf.currentTerm, reply.Term, server)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					rf.convert2Follower()
				} else {
					// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
					rf.nextIndex[server] = reply.NextIndex
				}
			}
		}
		rf.mu.Unlock()
	} else {
		DPrintf("[sendAppendEntries] server[%d](leader) cannot receive from server[%d].", rf.me, server)
	}
	return ok
}

func (rf *Raft) applyMsg() {
	DPrintf("[applyMsg] server[%d](state: %d) begins applying logs. lastApplied: %d", rf.me, rf.state, rf.lastApplied)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			Index:   rf.lastApplied,
			Command: rf.logs[rf.lastApplied].Command,
		}
		DPrintf("[applyMsg] server[%d] applies logs[%d].", rf.me, rf.lastApplied)
	}
	DPrintf("[applyMsg] server[%d](state: %d) ends applying logs. lastApplied: %d", rf.me, rf.state, rf.lastApplied)
}


// 一些踩过的坑：
// 发现Call函数，如果network断了会被阻塞(实际上是设置了一个比较长的delay时间，而且是随机的)，不用去管什么时候从Call返回，返回时ok也是false
// 在RV和AE中reset timer的时机需要仔细考虑，目前观察是，RV中仅voteGranted为true时才reset timer; AE中仅reply success及reply false但因为term mismatch的情况下才reset timer
// logs里必须先插入一个empty log, index从1开始，因为框架代码中会根据这个来判断是否reach an agreement

// 关于assignment3中unreliable相关test的解读：模拟网络很不稳定的情况，经常有节点断联。因此，需要使用针对更新nextIndex的优化，在一次心跳包
// 的返回中直接更新到相应位置。否则仅使用每次将nextIndex减1的未优化方法在网络不稳定经常丢包的情况下很难得完成同步。

// 艰难debug的问题：发现偶尔会出现apply out of order的错误，这说明有不止一个线程在apply log。apply的时候要加锁

// 网络不稳定的时候总是有节点容易变成candidate，通过增加election timeout到350-500之间、同时减小heartbeat timeout来更快达到共识

// go里不带缓冲的信道的机制是：只有消费者消费了数据，生产者才会返回，否则会一直阻塞。所以需要注意是否会因为锁的问题导致接收方不能接收数据，发送者一直被阻塞。
// 发现困扰自己最久的一个bug是，往resetTimerCh里放数据时没有判断是不是follower。所以导致偶尔出现，如果一个candidate被reset timer了，而此
// 时candidate loop中获取不到锁无法执行，又没有消费者消费，所以导致阻塞，Start函数无法返回，因此也无法因为测试中的超时失败退出。
