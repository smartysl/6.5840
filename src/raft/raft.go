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
	"sort"
	"bytes"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	RoleLeader = "RoleLeader"
	RoleCandidate = "RoleCandidate"
	RoleFollower = "RoleFollower"

	ActiveTimeout = 100 * time.Millisecond
	BroadcastInterval = 10 * time.Millisecond
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor int
	log []LogEntry

	// Valatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex []int
	matchIndex []int

	// Attributes related to election
	Role string
	LeaderId int
	LastActiveTime time.Time
	LastBroadcastTime time.Time

	// For 2B
	applyCh chan ApplyMsg

	// For 2D
	snapshotIndex int
	snapshotTerm int
}

type LogEntry struct {
	Command interface{}
	Term int
}

func (rf *Raft) index2LogPos(index int) int {
	return index - rf.snapshotIndex - 1
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) + rf.snapshotIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.LeaderId == rf.me
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
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftstate, snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.snapshotIndex)
	d.Decode(&rf.snapshotTerm)
	d.Decode(&rf.log)
	DPrintf("Raft instance %v recovered from persist, currentTerm: %v, votedFor: %v, lastLogIndex: %v", rf.me, rf.currentTerm, rf.votedFor, rf.lastLogIndex())
}

func (rf *Raft) persistSnapshot(data []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, data)
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	DPrintf("Raft instance %v called Snapshot, index: %v", rf.me, index)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.snapshotIndex {
		return
	}

	rf.snapshotTerm = rf.log[rf.index2LogPos(index)].Term
	rf.log = rf.log[rf.index2LogPos(index) + 1:]
	rf.snapshotIndex = index

	rf.persistSnapshot(snapshot)
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		// Has voted for a higher term
		return
	}
	
	if rf.currentTerm < args.Term {
		// Become the follower of this term
		rf.Role = RoleFollower
		rf.currentTerm = args.Term
		rf.LeaderId = -1
		rf.votedFor = -1
		
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// Haven't vote for anybody in this term

		lastLogTerm := rf.snapshotTerm
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log) - 1].Term
		}

		DPrintf("Raft instance %v is going to vote for %v, term: %v, local lastLogTerm: %v, local lastLogIndex: %v, args lastLogTerm: %v, args LastLogIndex: %v",
		 rf.me, args.CandidateId, args.Term, lastLogTerm, rf.lastLogIndex(), args.LastLogTerm, args.LastLogIndex)

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastLogIndex()) {
			// candidate's log is at least as up-to-date as receiver's log
			// vote for it
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			reply.VoteGranted = true
			reply.Term = args.Term

			rf.persist()

			rf.LastActiveTime = time.Now()
		} 
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

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Appended bool
	XTerm int
	XIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Raft instance %v has received AppendEntries request, leaderId: %v, term: %v, prevLogIndex: %v prevLogTerm: %v", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)

	reply.Appended = false
	reply.Term = rf.currentTerm
	reply.XTerm = -1
	reply.XIndex = -1
	
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.Role = RoleFollower
		rf.votedFor = -1
		rf.persist()
	}

	rf.LeaderId = args.LeaderId

	rf.LastActiveTime = time.Now()
	DPrintf("Raft instance %v has updated last active time:%v", rf.me, rf.LastActiveTime.UnixMilli())

	if args.PrevLogIndex < rf.snapshotIndex {
		reply.XIndex = 1
		return
	}

	if args.PrevLogIndex == rf.snapshotIndex && args.PrevLogTerm != rf.snapshotTerm {
		reply.XIndex = 1
		return
	}

	// If there is no local prev log, don't append
	if rf.lastLogIndex() < args.PrevLogIndex {
		reply.XIndex = rf.lastLogIndex() + 1
		return
	}

	// Local prev log's term must be equal to arg's prev log term
	if rf.index2LogPos(args.PrevLogIndex) > 0 && rf.log[rf.index2LogPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
		for i := 0; i <= rf.index2LogPos(args.PrevLogIndex); i++ {
			if rf.log[i].Term == reply.XTerm {
				reply.XIndex = i + 1 + rf.snapshotIndex
				break
			}
		}
		return
	}

	DPrintf("Raft instance %v append %v new entries", rf.me, len(args.Entries))

	// Catch up to the log
	rf.log = rf.log[:rf.index2LogPos(args.PrevLogIndex) + 1]
	rf.log = append(rf.log, args.Entries...)

	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.lastLogIndex() < rf.commitIndex {
			rf.commitIndex = rf.lastLogIndex()
		}
	}
	reply.Appended = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) increaseCommitIndex() {
	// Find the majority match index
	matchIndexes := make([]int, 0)
	// Add self progress
	matchIndexes = append(matchIndexes, rf.lastLogIndex())
	for peerId, _ := range rf.peers {
		if peerId == rf.me {
			continue
		}
		matchIndexes = append(matchIndexes, rf.matchIndex[peerId])
	}
	sort.Ints(matchIndexes)
	majorityMatchIndex := matchIndexes[len(rf.peers) / 2]
	if majorityMatchIndex > rf.commitIndex && (majorityMatchIndex <= rf.snapshotIndex || rf.log[rf.index2LogPos(majorityMatchIndex)].Term == rf.currentTerm) {
		DPrintf("Raft instance %v updates commit index to %v", rf.me, majorityMatchIndex)
		rf.commitIndex = majorityMatchIndex
	}
}

func (rf *Raft) doAppendEntries(id int) {
	rf.mu.Lock()

	args := &AppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		LeaderCommit: rf.commitIndex,
		Entries: make([]LogEntry, 0),
	}
	args.PrevLogIndex = rf.nextIndex[id] - 1
	if args.PrevLogIndex <= rf.snapshotIndex {
		args.PrevLogTerm = rf.snapshotTerm
	} else {
		args.PrevLogTerm  = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
	}

	args.Entries = append(args.Entries, rf.log[rf.index2LogPos(rf.nextIndex[id]):]...)

	reply := &AppendEntriesReply{}

	rf.mu.Unlock()
	
	if ok := rf.sendAppendEntries(id, args, reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm != args.Term {
			return
		}
		if reply.Term > rf.currentTerm {
			// There has already been a new leader
			rf.currentTerm = reply.Term
			rf.LeaderId = -1
			rf.Role = RoleFollower
			rf.votedFor = -1

			rf.persist()
			return
		}

		//Reduce the next index and retry
		if !reply.Appended {
			rf.nextIndex[id] = reply.XIndex
			return
		}

		rf.nextIndex[id] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[id] = rf.nextIndex[id] - 1

		rf.increaseCommitIndex()
	}
}

func (rf *Raft) appendEntriesLoop() {
	for rf.killed() == false {

		rf.mu.Lock()

		if rf.Role != RoleLeader {
			rf.mu.Unlock()
			time.Sleep(1 * time.Millisecond)
			continue
		}

		if time.Now().Sub(rf.LastBroadcastTime) < BroadcastInterval {
			rf.mu.Unlock()
			time.Sleep(1 * time.Millisecond)
			continue
		}
		rf.LastBroadcastTime = time.Now()

		DPrintf("Raft instance %v start to append entries, current term: %v", rf.me, rf.currentTerm)

		rf.mu.Unlock()

		for peerId, _ := range rf.peers {
			if peerId == rf.me {
				continue
			}
			if rf.nextIndex[peerId] <= rf.snapshotIndex {
				go rf.doInstallSnapshot(peerId)
			} else {
				go rf.doAppendEntries(peerId)
			}
		}

		time.Sleep(20 * time.Millisecond)
	}
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.Role != RoleLeader {
		return index, term, false
	}

	entry := LogEntry{
		Command: command,
		Term: rf.currentTerm,
	}
	rf.log = append(rf.log, entry)
	index = rf.lastLogIndex()
	term = rf.currentTerm

	rf.persist()

	DPrintf("Raft instance %v calls Start, index: %v, term: %v", rf.me, index, term)

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
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock()

		now := time.Now()

		DPrintf("Current timestamp: %v, raft instance %v, current term: %v, last active time: %v", now.UnixMilli(), rf.me, rf.currentTerm, rf.LastActiveTime.UnixMilli())

		if rf.Role == RoleFollower && now.Sub(rf.LastActiveTime) > ActiveTimeout {
			rf.Role = RoleCandidate
		}

		// Start election
		if rf.Role == RoleCandidate {

			DPrintf("The raft instance %v starts election", rf.me)

			// Vote for myself
			rf.LastActiveTime = now
			rf.currentTerm++
			rf.votedFor = rf.me

			lastLogTerm := rf.snapshotTerm
			if len(rf.log) > 0 {
				lastLogTerm = rf.log[len(rf.log) - 1].Term
			}
			args := &RequestVoteArgs {
				Term: rf.currentTerm,
				CandidateId: rf.me,
				LastLogIndex: rf.lastLogIndex(),
				LastLogTerm: lastLogTerm,
			}

			// Request votes
			// Release lock at first
			rf.mu.Unlock()
			voteResults := make(chan *RequestVoteReply, len(rf.peers) - 1)
			for peerId, _ := range rf.peers {
				go func (id int) {
					if id == rf.me {
						return
					}
					resp := &RequestVoteReply{}
					if ok := rf.sendRequestVote(id, args, resp); ok {
						voteResults <- resp
					} else {
						voteResults <- nil
					}
				}(peerId)
			}

			// Gather the vote result
			maxTerm := 0
			approvedCnt := 1
			totalCnt := len(rf.peers) - 1
			waitVote := true
			for waitVote {
				select {
					case voteResult := <- voteResults:
						totalCnt--
						if voteResult != nil {
							DPrintf("Raft instance %v receive vote reply %v", rf.me, voteResult)

							if voteResult.VoteGranted {
								approvedCnt++
							}
							if voteResult.Term > maxTerm {
								maxTerm = voteResult.Term
							}
						} else {
							DPrintf("Raft instance %v receive uncertain vote reply", rf.me)
						}
						if totalCnt == 0 || approvedCnt > len(rf.peers) / 2 {
							waitVote = false
							break
						}
				}
			}

			rf.mu.Lock()
			
			// If no longer candidate, skip voting
			if rf.Role != RoleCandidate {
				rf.mu.Unlock()
				ms := 50 + (rand.Int63() % 300)
				time.Sleep(time.Duration(ms) * time.Millisecond)
				continue
			}
			
			// If other candidate has announced higher term, turn back to follower
			if maxTerm > rf.currentTerm {
				rf.Role = RoleFollower
				rf.LeaderId = -1
				rf.currentTerm = maxTerm
				rf.votedFor = -1

				rf.persist()

				rf.mu.Unlock()
				ms := 50 + (rand.Int63() % 300)
				time.Sleep(time.Duration(ms) * time.Millisecond)
				continue
			}

			// I'm the fucking leader
			if approvedCnt > len(rf.peers) / 2 {
				DPrintf("Raft instance %v has won the election", rf.me)

				rf.Role = RoleLeader
				rf.LeaderId = rf.me
				rf.LastBroadcastTime = time.Unix(0, 0)
			}

			rf.persist()
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyLogLoop() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			DPrintf("Raft instance %v try to apply log, commitIndex: %v, lastApplied: %v, snapshotIndex: %v", rf.me, rf.commitIndex, rf.lastApplied, rf.snapshotIndex)
			msg := ApplyMsg{
				CommandValid: true,
				Command: rf.log[rf.index2LogPos(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
				CommandTerm: rf.log[rf.index2LogPos(rf.lastApplied)].Term,
			}
			DPrintf("Raft instance %v apply msg to application level, command index: %v", rf.me, msg.CommandIndex)
			rf.applyCh <- msg
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Offset int
	Data []byte
	Done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) doInstallSnapshot(id int) {
	args := &InstallSnapshotArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm: rf.snapshotTerm,
		Data: rf.persister.ReadSnapshot(),
		Done: true,
	}

	reply := &InstallSnapshotReply{}

	DPrintf("Raft instance %v send install snapshot request to %v, term: %v, lastIncludedIndex: %v, lastIncludedTerm: %v", rf.me, id, rf.currentTerm, rf.snapshotIndex, rf.snapshotTerm)

	if ok := rf.sendInstallSnapshot(id, args, reply); ok {

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.Role = RoleFollower
			rf.LeaderId = -1
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			return
		}

		rf.nextIndex[id] = rf.lastLogIndex() + 1
		rf.matchIndex[id] = args.LastIncludedIndex
		DPrintf("Raft instance %v update nextIndex[%v] to %v, matchIndex[%v] to %v", rf.me, id, rf.nextIndex[id], id, rf.matchIndex[id])
		rf.increaseCommitIndex()
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.Role = RoleFollower
		rf.votedFor = -1
		rf.persist()
	}

	rf.LeaderId = args.LeaderId
	rf.LastActiveTime = time.Now()

	if args.LastIncludedIndex <= rf.snapshotIndex {
		// Outdated snapshot
		return
	}

	DPrintf("Raft instance %v install snapshot, term: %v, lastIncludedIndex: %v, lastIncludedTerm: %v", rf.me, rf.currentTerm, args.LastIncludedIndex, args.LastIncludedTerm)

	if rf.lastLogIndex() <= args.LastIncludedIndex {
		rf.log = make([]LogEntry, 0)
	} else {
		if rf.log[rf.index2LogPos(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
			rf.log = make([]LogEntry, 0)
		} else {
			rf.log = rf.log[rf.index2LogPos(args.LastIncludedIndex) + 1:]
		}
	}

	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm

	rf.persistSnapshot(args.Data)

	rf.applySnapshot()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) applySnapshot() {
	applyMsg := ApplyMsg{
		CommandValid: false,
		SnapshotValid: true,
		Snapshot: rf.persister.ReadSnapshot(),
		SnapshotIndex: rf.snapshotIndex,
		SnapshotTerm: rf.snapshotTerm,
	}
	DPrintf("Read snapshot length: %v", len(applyMsg.Snapshot))
	rf.lastApplied = rf.snapshotIndex
	rf.applyCh <- applyMsg
}

func (rf *Raft) ReadSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.Role = RoleFollower
	rf.LeaderId = -1
	rf.votedFor = -1
	rf.LastActiveTime = time.Unix(0, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	for peerId, _ := range peers {
		rf.nextIndex[peerId] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// update lastApplied to snapshot index
	rf.lastApplied = rf.snapshotIndex

	// start ticker goroutine to start elections
	go rf.ticker()

	// keep send heartbeat as leader
	go rf.appendEntriesLoop()

	go rf.applyLogLoop()


	return rf
}
