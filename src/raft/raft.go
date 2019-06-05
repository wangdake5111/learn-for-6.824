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
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"



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
const(
	FOLLOWER = iota
	CANDIDATE
	LEADER
	HEARTBEATINTERVAL = 40*time.Millisecond
)
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type Log struct {
	Command interface{}
	Term 	int
	Index 	int
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	//log 	[]Log
	voteCount int
	currentTerm int
	votedFor int
	//leader	int
	state int
	//chanAppend chan bool
	//chanRequest chan bool
	resetTimer        chan struct{}
	electionTimer     *time.Timer
	electionTimeOut   time.Duration
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state==LEADER)
	// Your code here (2A).
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
	Term int
	CandidateId int

	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	Success bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term<rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
	}else {
		if args.Term>rf.currentTerm{
			//find a larger term ,turn to be a follower,voteFor = -1
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		}
		if rf.votedFor == -1{
			// have vote,reset the timer
			rf.resetTimer <- struct{}{}
			rf.state = FOLLOWER
			rf.votedFor = args.CandidateId
			reply.Success = true
			DPrintf("[%d]: peer %d vote to peer %d",rf.me,rf.me,args.CandidateId)
		}
	}
}
type AppendEntriesArgs struct {
	Term int
	LeaderId int
}
type AppendEntriesReply struct {
	Term int
	Success bool
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	DPrintf("[%d-]: rpc AE, from peer: %d, term: %d\n", rf.me, args.LeaderId, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// appendEntries rule 1
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//all server rule 1
	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
	}
	// just one leader
	if rf.state == LEADER{
		rf.turnToFollow()
	}
	// vote for the true leader
	if rf.votedFor != args.LeaderId{
		rf.votedFor = args.LeaderId
	}
	//get append,reset the timer
	rf.resetTimer <- struct{}{}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) fillRequestVoteArgs(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// turn to candidate and vote to itself
	rf.votedFor = rf.me
	rf.currentTerm += 1
	rf.state = CANDIDATE

	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	//args.LastLogIndex, args.LastLogTerm = rf.lastLogIndexAndTerm()
}
func (rf *Raft) fillAppendEntriesArgs(args *AppendEntriesArgs){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args.Term = rf.currentTerm
	args.LeaderId = rf.me
}
func (rf *Raft) turnToFollow(){
	rf.state=FOLLOWER
	rf.votedFor = -1
}
// check the servers' consistency
func (rf *Raft) consistencyCheckReplyHandler(n int, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER{
		return
	}
	if reply.Success{

	}else{
		//为什么还要检验rf.state
		if(rf.state==LEADER && reply.Term>rf.currentTerm){
			//find a larger term, turn to be a follower
			rf.turnToFollow()
			rf.resetTimer <- struct{}{}
			DPrintf("[%d-]: leader %d found new term (heartbeat resp from peer %d), turn to follower.",
				rf.me, rf.me, n)
			return
		}
	}
}
// heartbeat for ensure leader's state
func (rf *Raft)heartbeatDaemon() {
	for {
		// not a leader, out!
		if _, isLeader := rf.GetState(); !isLeader {
			return
		}
		//send a heartbeat, reset the timer
	rf.resetTimer <- struct{}{}
	var args AppendEntriesArgs
	rf.fillAppendEntriesArgs(&args)
	select {
	//case <-rf.chanAppend:
	default:
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(n int) {
					var reply AppendEntriesReply
					if rf.sendAppendEntries(n, &args, &reply){
						// if rpc has been sent, check the consistency
						rf.consistencyCheckReplyHandler(n, &reply)
					}
				}(i)
			}
		}
	}
	time.Sleep(HEARTBEATINTERVAL)
}
}
func (rf *Raft) broadcastRequestVotes(){
	var args RequestVoteArgs
	rf.fillRequestVoteArgs(&args)
	peers := len(rf.peers)
	vote := 1
	// a handler to ensure whether a candidate can be a leader
	replyHandler := func(reply *RequestVoteReply){
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// the "if" is to prevent the situation that another candidate be a leader when the go routines still run
		if rf.state==CANDIDATE{
			if reply.Term > args.Term{
				rf.currentTerm = reply.Term
				rf.turnToFollow()
				rf.resetTimer <- struct{}{}
				return
			}
			if reply.Success {
				//why == peers/2 is because vote++ is at the end
				if vote == peers/2{
					rf.state = LEADER
					//when it is a leader, it should send the heartbeat periodically
					go rf.heartbeatDaemon()
					DPrintf("[%d-]: peer %d become new leader.\n", rf.me,rf.me)
					return
				}
				vote++
			}
		}
	}
	for i:=0;i<peers;i++ {
		if(rf.me!=i){
			go func(n int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(n, &args, &reply){
					replyHandler(&reply)
				}
			}(i)
		}
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}
func (rf*Raft) electionDaemon(){
	for {
		select {
		// the signal to resetTimer
		case <-rf.resetTimer:
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(rf.electionTimeOut)
			//timeout, start to elect the leader
		case <-rf.electionTimer.C:
			DPrintf("[%d]: peer %d election timeout, issue election @ term %d\n",
				rf.me, rf.me, rf.currentTerm)
			// request for the vote and reset the timer
			go rf.broadcastRequestVotes()
			rf.electionTimer.Reset(rf.electionTimeOut)
		}
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//initialize the raft
	rf.state=FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	//rf.chanAppend = make(chan bool)
	//rf.chanRequest = make(chan bool)
	rf.resetTimer = make(chan struct{})
	rf.electionTimeOut = time.Duration(400+rand.Int63()%200)*time.Millisecond
	rf.electionTimer = time.NewTimer(rf.electionTimeOut)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//start the election
	go rf.electionDaemon()
	DPrintf("[%d-]: newborn election(%s) heartbeat(%s) term(%d) voted(%d)\n",
		rf.me, rf.electionTimeOut, HEARTBEATINTERVAL, rf.currentTerm, rf.votedFor)


	return rf
}
