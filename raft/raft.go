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
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           string
	term            int
	votedFor        int
	electionTimeOut *time.Timer
	raftLog         []RaftLogContents
	commitIndex     int
	applyCh         chan ApplyMsg
	//leader only
	hbTimeOut             *time.Timer
	nextIndex             map[int]int
	entryCount            int
	masterRaftLogContents []raftLogAdditional
}

//RaftLogContents
type RaftLogContents struct {
	Index              int
	Term               int
	Command            interface{}
	AdditionalContents raftLogAdditional
}

type raftLogAdditional struct {
	Acks     int
	Commited bool
}

//decl of constants
const (
	leader    = "leader"
	follower  = "follower"
	candidate = "candidate"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	if rf.state == leader {
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateID int
	Term        int
	CommitIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	Vote bool
}

type PrevLogEntry struct {
	Term  int
	Index int
}

//AppendEntriesArgs
type AppendEntriesArgs struct {
	Term        int
	CandidateID int
	CommitEntry PrevLogEntry
	LogEntries  []RaftLogContents
	PrevEntry   PrevLogEntry
}

//AppendEntriesReply
type AppendEntriesReply struct {
	Term     int
	Accepted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v.%v] Recv RequestVote with %v", rf.me, rf.term, args)
	switch {
	case args.Term <= rf.term:
		reply.Vote = false
		reply.Term = rf.term
	case args.CommitIndex < rf.commitIndex:
		reply.Vote = false
		rf.term = args.Term
	default:
		rf.state = follower
		reply.Vote = true
		rf.term = args.Term
		rf.votedFor = args.CandidateID
		rf.electionTimeOut.Reset(time.Duration(rf.me+1) * time.Second)
	}
	DPrintf("[%v.%v] Recv Replied with %v", rf.me, rf.term, reply)
}

func (rf *Raft) commitLogs(commit int) {
	for i := rf.commitIndex + 1; i <= commit; i++ {
		rf.applyCh <- ApplyMsg{CommandValid: true,
			Command:      rf.raftLog[i].Command,
			CommandIndex: i + 1}
		DPrintf("[%v.%v] Commited in follower index is %v", rf.me, rf.term, i+1)
	}
	rf.commitIndex = commit
}

//AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer rf.mu.Unlock()
	rf.mu.Lock()
	reply.Term = rf.term
	reply.Accepted = true
	if rf.term <= args.Term {
		rf.state = follower
		rf.term = args.Term
		//check other contents
		if len(args.LogEntries) != 0 {
			if rf.canAppend(&args.PrevEntry) {
				rf.raftLog = rf.raftLog[:args.PrevEntry.Index]
				rf.raftLog = append(rf.raftLog, args.LogEntries...)
				reply.Accepted = true
				DPrintf("[%v.%v] Appended to log and len of log is %v", rf.me, rf.term, len(rf.raftLog))
			} else {
				reply.Accepted = false
				DPrintf("[%v.%v] Reject by follower for prev index %v", rf.me, rf.term, args.PrevEntry.Index)
			}
		}
		if rf.canAppend(&args.CommitEntry) {
			rf.commitLogs(args.CommitEntry.Index - 1)
		} else {
			reply.Accepted = false
		}
		rf.electionTimeOut.Reset(time.Duration(rf.me+1) * time.Second)
	} else {
		reply.Accepted = false
	}
}

func (rf *Raft) canAppend(prev *PrevLogEntry) bool {
	var result bool
	if prev.Index > len(rf.raftLog) {
		result = false
	} else if len(rf.raftLog) == 0 {
		result = true
	} else if prev.Index == 0 {
		result = true
	} else if rf.raftLog[prev.Index-1].Term == prev.Term && rf.raftLog[prev.Index-1].Index == prev.Index {
		result = true
	}

	return result
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, term int) {
	rf.mu.Lock()
	//newTerm := rf.term
	//state := rf.state
	DPrintf("[%v.%v] Sent request vote to %v", rf.me, rf.term, server)
	rf.mu.Unlock()
	//var ok bool
	//for !ok && term == newTerm && state == candidate {
	rf.peers[server].Call("Raft.RequestVote", args, reply)
	//check if no reply is recvd
	/*
		if !ok {
			rf.mu.Lock()
			newTerm = rf.term
			state = rf.state
			rf.mu.Unlock()
		}
	*/
	//}
	//return ok
}

//call AppendEntries
func (rf *Raft) callAppendEntries(server int, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	if state == leader {
		rf.mu.Lock()
		args := &AppendEntriesArgs{CandidateID: rf.me, Term: rf.term}
		if rf.commitIndex >= 0 {
			args.CommitEntry = PrevLogEntry{Index: rf.raftLog[rf.commitIndex].Index, Term: rf.raftLog[rf.commitIndex].Term}
		}
		if rf.nextIndex[server] < len(rf.raftLog) {
			if len(rf.raftLog) == 1 {
				args.LogEntries = rf.raftLog[rf.nextIndex[server]:]
			} else if len(rf.raftLog) > 1 {
				args.LogEntries = rf.raftLog[rf.nextIndex[server]:]
				if rf.nextIndex[server] != 0 {
					args.PrevEntry.Index = rf.raftLog[rf.nextIndex[server]-1].Index
					args.PrevEntry.Term = rf.raftLog[rf.nextIndex[server]-1].Term
				}
			}
		}
		var sentLen int
		sentLen = len(rf.raftLog)
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		rf.mu.Lock()
		if reply.Accepted && len(args.LogEntries) != 0 {
			DPrintf("[%v.%v]Got a conform Append ack from %v for till index %v", rf.me, rf.term, server, sentLen)
			rf.acceptedEntries()
			rf.nextIndex[server] = sentLen
		}
		rf.mu.Unlock()
		return ok
	}
	return true
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.raftLog) + 1
	term := rf.term
	var isLeader bool
	if rf.state == leader {
		rf.entryCount++
		entryID := strconv.Itoa(rf.me) + "_" + strconv.Itoa(rf.entryCount)
		isLeader = true
		rf.raftLog = append(rf.raftLog, RaftLogContents{Index: index, Term: term, Command: command, AdditionalContents: raftLogAdditional{Acks: 1, Commited: false}})
		DPrintf("Created From node %v and the entry ID is %v and value is %v", rf.me, entryID, command)
	}
	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
func (rf *Raft) isMajority(count int) bool {
	if float64(count) >= math.Ceil(float64(len(rf.peers))/2) {
		return true
	}
	return false
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("[%v.%v]Term timeout and its state %v", rf.me, rf.term, rf.state)
	rf.term++
	term := rf.term
	rf.state = candidate
	rf.votedFor = rf.me
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	//Init values
	args := &RequestVoteArgs{CommitIndex: commitIndex, CandidateID: rf.me, Term: term}
	vote := 1                          //No of positive votes
	voted := 1                         //No of nodes responded to RequestVote
	var voteMU sync.Mutex              //Lock for vote variables
	voteCheck := sync.NewCond(&voteMU) //Lock for vote condition variable
	DPrintf("[%v] Start a election for term %v", rf.me, term)
	//Send RequestVote
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			//
			reply := &RequestVoteReply{}
			rf.sendRequestVote(i, args, reply, term)
			voteMU.Lock()
			voted++
			if reply.Vote {
				vote++
			}
			voteMU.Unlock()
			voteCheck.Broadcast()
		}(i)
	}
	voteMU.Lock()
	for {
		if rf.isMajority(vote) || rf.isMajority(voted-vote) {
			break
		}
		voteCheck.Wait()
	}
	if rf.isMajority(vote) {
		rf.mu.Lock()
		rf.state = leader
		DPrintf("Leader made %v in term %v", rf.me, rf.term)
		//Assign next Values
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.raftLog)
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		rf.state = follower
		rf.mu.Unlock()
	}
	voteMU.Unlock()
}

func (rf *Raft) sendHbs() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &AppendEntriesReply{}
			//If failed to reach try again
			//resp := false
			//for !resp {
			rf.callAppendEntries(i, reply)
			//}
			rf.mu.Lock()
			if reply.Term > rf.term {
				DPrintf("[%v.%v] Saw a greater term[%v] and changed state to follower", rf.me, rf.term, reply.Term)
				rf.state = follower
				rf.term = reply.Term
			} else if !reply.Accepted {
				//DPrintf("[%v.%v] Reduced the nextIndex for follower %v and its reply is %v", rf.me, rf.term, i, reply)
				if rf.nextIndex[i] != 0 {
					rf.nextIndex[i]--
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) acceptedEntries() {
	//based on LogMatchingProperty
	for i := rf.commitIndex + 1; i < len(rf.raftLog); i++ {
		//rf.masterRaftLogContents[i].Acks++
		rf.raftLog[i].AdditionalContents.Acks++
		if rf.isMajority(rf.raftLog[i].AdditionalContents.Acks) && !rf.raftLog[i].AdditionalContents.Commited {
			rf.commitIndex = i
			rf.raftLog[i].AdditionalContents.Commited = true
			DPrintf("[%v] Commited at leader log index is %v", rf.me, rf.raftLog[i].Index)
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.raftLog[i].Command, CommandIndex: i + 1}
		}
	}
}

func (rf *Raft) electionTimer() {
	for {
		select {
		case <-rf.electionTimeOut.C:
			//On expiry
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			//If state is not leader call election with new term
			if state != leader {
				go rf.startElection()
			}
			rf.electionTimeOut.Reset(time.Duration(rf.me+1) * time.Second) //Reset Election timeout

		case <-rf.hbTimeOut.C:
			//Om handleTimerExpiry
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			//If state is not leader call election with new term
			if state == leader {
				go rf.sendHbs()
			}
			rf.hbTimeOut.Reset(50 * time.Millisecond) //Rest HeartBeat timer
		}
	}
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = follower
	rf.nextIndex = make(map[int]int, len(peers))
	rf.applyCh = applyCh
	rf.commitIndex = -1
	rf.electionTimeOut = time.NewTimer(time.Duration(rf.me+1) * time.Second) //Election timeout
	rf.hbTimeOut = time.NewTimer(50 * time.Millisecond)                      //HeartBeat timer for leader
	// Your initialization code here (2A, 2B, 2C).
	DPrintf("I'm in %v and its len %v", rf.me, len(rf.peers))
	go rf.electionTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
