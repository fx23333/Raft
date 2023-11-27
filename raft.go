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

import "sync"
import "sync/atomic"
import "../labrpc"
import "math/rand"
import "time"
//import "fmt"
import "bytes"
import "../labgob"


const FOLLOWER = 0
const CANDIDATE = 1
const LEADER = 2
const ELECTIONINTERVAL = 300       //300             //50
const ELECTIONBEGIN = 700           //300           //500

//允许的范围内，这两个值越小越好
const ELECTIONCHECKINTERVAL = 10   //50             //10
const HEATBEATINTERVAL = 100                        //100




func min(a int, b int) int {
	if (a <= b) {
		return a
	} else {
		return b
	}
}

func max(a int, b int) int {
	if (a >= b) {
		return a
	} else {
		return b
	}
}

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
    electionnotTimeOut int              


	lastVisitTime  time.Time
	currentTerm    int
	voteFor        int
	state          int
	log            []interface{}
	logTerm        []int
	commitIndex    int
	nextIndex      []int                //需要初始化
	matchIndex     []int                //需要初始化
	logAccept      []int
	lastApplied    int

	applyCh chan ApplyMsg


	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}


func getRandomElectionnotTimeOut() (int) {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(ELECTIONINTERVAL) + ELECTIONBEGIN
	return n
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	isleader = (rf.state == LEADER)
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	//本函数不会上锁，需要在外部上锁


	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//rf.mu.Lock()
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(len(rf.log))
	//DPrintf("Encoding! term = %v, voteFor = %v, log = %v\n", rf.currentTerm, rf.voteFor, rf.log)
	for _, logEntry := range rf.log {
		//DPrintf("I am encoding %v\n", logEntry)

		switch tempLogEntry := logEntry.(type) {
		case int:       e.Encode(tempLogEntry)
		case string:    e.Encode(tempLogEntry)
		}
	}

	for _, logTerm := range rf.logTerm {
		e.Encode(logTerm)
	}

	//rf.mu.Unlock()
	data := w.Bytes()
	//DPrintln(data)
	rf.persister.SaveRaftState(data)

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


	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tempCurrentTerm, tempVoteFor, logLength int
	var tempLog []interface{}
	var tempLogTerm []int
	if d.Decode(&tempCurrentTerm) != nil ||
	   d.Decode(&tempVoteFor) != nil || d.Decode(&logLength) != nil{
		DPrintf("Decode Error!\n")
		return
	} else {
	    // var tempEntry interface{}
		// var tempEntryTerm int
		//DPrintf("term = %v, votefor = %v, length = %v\n", tempCurrentTerm, tempVoteFor, logLength)
		//DPrintf("real: %v, %v, %v\n\n", rf.currentTerm, rf.voteFor, rf.log)
		for i := 0; i < logLength; i++ {
			var tempEntry int
			if (d.Decode(&tempEntry) != nil) {
				DPrintf("Decode Error2! %v\n", i)
				return
			} else {
				tempLog = append(tempLog, tempEntry)
			}
		}


		for i := 0; i < logLength; i++ {
			var tempEntryTerm int
			if (d.Decode(&tempEntryTerm) != nil) {
				DPrintf("Decode Error3!\n")
				return
			} else {
				tempLogTerm = append(tempLogTerm, tempEntryTerm)
			}
		}
	}


	rf.mu.Lock()
	rf.currentTerm = tempCurrentTerm
	rf.voteFor = tempVoteFor
	rf.log = make([]interface{}, len(tempLog))
	rf.logTerm = make([]int, len(tempLogTerm))
	copy(rf.log, tempLog)
	copy(rf.logTerm, tempLogTerm)
	rf.persist()
	rf.mu.Unlock()
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntriesArgs struct {
    Term int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
	Entries     []interface{}
	LogTerm     []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
    ConflictedIndex int
	ConflictedTerm  int
	Loglength       int
	Retry           bool   //如果retry为true，不需要修改args，直接重发
}

func updateCommitIndex(rf *Raft, commitIndex int) {
	
// 	if (rf.commitIndex < commitIndex) {
// //		DPrintln("Hiiiiiii")
// 		rf.commitIndex = commitIndex
// 	}
	rf.commitIndex = min(commitIndex, len(rf.log) - 1)
}

func ElectionRestrictionCheck(rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//本函数无锁，需要在外部上锁
	var myterm int
	mylength := len(rf.log) - 1
	if (mylength == -1) {
		myterm = -200
	} else {
		myterm = rf.logTerm[mylength]
	}



	if (myterm > args.LastLogTerm) {
		DPrintf("ER S3: me: %v, mylastlogterm = %v, candilastlogterm = %v, candidate = %v\n", rf.me, myterm, args.LastLogTerm, args.CandidateID)
		reply.VoteGranted = false
	} else if (myterm < args.LastLogTerm) {
		rf.lastVisitTime = time.Now()
	    rf.voteFor = args.CandidateID
		rf.persist()
	    reply.VoteGranted = true
		DPrintf("ER S1: me: %v, mylastlogterm = %v, candilastlogterm = %v, candidate = %v\n", rf.me, myterm, args.LastLogTerm, args.CandidateID)
	} else {
		if (mylength > args.LastLogIndex) {
			DPrintf("ER S4: me: %v, mylastlogterm = %v, candilastlogterm = %v, candidate = %v\n", rf.me, myterm, args.LastLogTerm, args.CandidateID)
			reply.VoteGranted = false
		} else {
			rf.lastVisitTime = time.Now()
			rf.voteFor = args.CandidateID
			rf.persist()
			reply.VoteGranted = true
			DPrintf("ER S2: me: %v, mylastlogterm = %v, mylogterm = %v, candilastlogterm = %v, candidate = %v", rf.me, myterm, rf.logTerm, args.LastLogTerm, args.CandidateID)
			
		}
	}


   return reply.VoteGranted

}



//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if (args == nil) {
		reply.VoteGranted = false
		return
	}
    rf.mu.Lock()
	//DPrintf("%v(%v, %v) receive vote from %v(%v)\n", rf.me, rf.currentTerm, rf.state, args.CandidateID, args.Term)
	
	//DPrintf("%v receive %v\n", rf.me, args.CandidateID)
	if (rf.state == LEADER) {
		if (rf.currentTerm >= args.Term) {    //不承认新的leader
		    reply.VoteGranted = false
		} else { //承认新的leader
			//此处可能有问题，等于号也可能有问题
			//rf.currentTerm = args.Term
            rf.state = FOLLOWER
			//////rf.lastVisitTime = time.Now()
			//election restriction
			if (!ElectionRestrictionCheck(rf, args, reply)) {
				rf.voteFor = -1
				rf.persist()
			}
		}




	} else if (rf.state == FOLLOWER) {
		if (rf.currentTerm > args.Term) {    //不承认新的leader
		    reply.VoteGranted = false

		} else if (rf.currentTerm == args.Term ){    //已经投票给了这个candidate 或者还没投票

			//if (rf.voteFor == args.CandidateID || rf.voteFor == -1){
			if (rf.voteFor == -1){
				ElectionRestrictionCheck(rf, args, reply)
				//若检测通过，则自动设置Votefor，否则votefor不变
			} else {
				//此时，follower已经投过票了!如果遇到的不是当前投票的candidate，则一定不会投票。如果遇到的是已经投过的candidate，
				//不应该再去检查一致性，因为第二次可能会通不过。（或者在检查一致性，返回失败之后，不重设-1即可，否则可能会导致当前follower投出两票）
				//若是原始candidate重发，则返回false也无影响。因为在candidate的代码中，受到投票才会计数
				reply.VoteGranted = false
			}
		} else if (rf.currentTerm < args.Term) {
			//rf.currentTerm = args.Term
			if (!ElectionRestrictionCheck(rf, args, reply)) {
				rf.voteFor = -1
				rf.persist()
			}
		}



	} else {  //candiedate的情况
		if (rf.currentTerm >= args.Term) {    //不投票
		    reply.VoteGranted = false
		} else { //承认新的leader
			//DPrintln("hi")
			//rf.currentTerm = args.Term
            rf.state = FOLLOWER
			////////rf.lastVisitTime = time.Now()
			if (!ElectionRestrictionCheck(rf, args, reply)) {
				//DPrintf("I disagree\n")
				rf.voteFor = -1
				rf.persist()
			}
		}
	}
	rf.currentTerm = max(rf.currentTerm, args.Term)
	reply.Term = rf.currentTerm  //这句话必须放在最后。如果某个server状态更新之后再去投票，它返回的应该是更新的term，而不是原始term，否则会导致candidate丢掉投票
	rf.mu.Unlock()
	// Your code here (2A, 2B).
}


func ConsistencyCheck(rf *Raft, args *AppendEntriesArgs, reply *AppendEntriesReply) (bool){
	reply.Loglength = len(rf.log)
	if (len(rf.log) - 1 < args.PrevLogIndex && args.PrevLogIndex != -1) { //&& args.PrevLogIndex != -1可以删除
		//DPrintln("Consistency Check: S1: follower shorter than leader; me: ", rf.me, "prevlogindex = ", args.PrevLogIndex, "logsize = ", len(rf.log))
		reply.Loglength = len(rf.log)
		
		reply.Success = false


	} else if (args.PrevLogIndex != -1 && rf.logTerm[args.PrevLogIndex] != args.PrevLogTerm) {
		
		reply.ConflictedTerm = rf.logTerm[args.PrevLogIndex]
		// DPrintln("S2")


		//二分查找第一条数据,右区间左端点
		l := 0; r := args.PrevLogIndex
		var mid int
		for (l < r) {
			mid = (l + r) >> 1
			if (rf.logTerm[mid] >= rf.logTerm[args.PrevLogIndex]) {
				r = mid
			} else {
				l = mid + 1
			}
		}
		DPrintf("S2: me:%v, leader:%v, myprevlogterm: %v argsPrevlogterm%v, l: %v, r: %v, logTerm[l]=%v, logTerm[r] = %v", 
		rf.me, args.LeaderID, rf.logTerm[args.PrevLogIndex], args.PrevLogTerm, l, r, rf.logTerm[l], rf.logTerm[r])
		if (l - 1 >= 0) {
			DPrintf(", lastlogterm = %v\n", rf.logTerm[l - 1])	
		}
		reply.ConflictedIndex = r
		//DPrintf("S2 l = %v\n", l)
		reply.Success = false
	} else {
		//DPrintln("S3 ")
		//DPrintln("S3 ", args)




		if (len(args.Entries) != 0 ) {  //entries不为空
			lastEntryIndex := args.PrevLogIndex + len(args.Entries)
			//lastEntryTerm := args.LogTerm[len(args.Entries) - 1]	
			for i := args.PrevLogIndex + 1; i <= lastEntryIndex; i++ {
				if (i > len(rf.log) - 1) {
					rf.log     = append(rf.log    [: i], args.Entries[i - args.PrevLogIndex - 1: ]...)
					rf.logTerm = append(rf.logTerm[: i], args.LogTerm[i - args.PrevLogIndex - 1: ]...)
					break
				} else {
					a := rf.logTerm[i]
					b := args.LogTerm[i - args.PrevLogIndex - 1]
					if (a != b) {
						rf.log     = append(rf.log    [: i], args.Entries[i - args.PrevLogIndex - 1: ]...)
						rf.logTerm = append(rf.logTerm[: i], args.LogTerm[i - args.PrevLogIndex - 1: ]...)
						break
					}
				}
			}
		}


		//updateCommitIndex(rf, args.LeaderCommit)
		reply.Success = true
	}
	if (reply.Success) {
		updateCommitIndex(rf, args.LeaderCommit)
	}
	//updateCommitIndex(rf, args.LeaderCommit)
	return reply.Success
}


//插入
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if (args == nil) {
		reply.Success = false
		reply.ConflictedIndex = -1
		reply.ConflictedTerm = -1
		return 
	}
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if (rf.state == FOLLOWER) {
		if (rf.currentTerm > args.Term || rf.voteFor != args.LeaderID) {
			//DPrintf("S0 myterm = %v, leaderterm = %v, leader = %v\n", rf.currentTerm, args.Term, args.LeaderID)
			reply.ConflictedIndex = -1
			reply.ConflictedTerm = -1
			reply.Success = false
		} else {
			rf.currentTerm = args.Term   //这是CurrentTerm < args.Term的情况
			rf.lastVisitTime = time.Now()
			//rf.persist()
			ConsistencyCheck(rf, args, reply)
		}


	} else if (rf.state == CANDIDATE) {
		if (rf.currentTerm > args.Term) {
			//do nothing execpt for setting reply.success
			//leader过期
			reply.ConflictedIndex = -1
			reply.ConflictedTerm = -1
			reply.Success = false
			//DPrintln("S4")
		} else if (rf.currentTerm < args.Term) { //接受新的leader
			rf.voteFor = args.LeaderID
			rf.lastVisitTime = time.Now()
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			//rf.persist()
			ConsistencyCheck(rf, args, reply)


			//DPrintln("S5")
		} else { //rf.currentTerm == args.Term   //接受新的leader
			rf.lastVisitTime = time.Now()
			rf.voteFor = args.LeaderID
			rf.state = FOLLOWER
			//DPrintln("S6")
			//rf.persist()
			ConsistencyCheck(rf, args, reply)
		}

	} else {   //leader
		if (rf.currentTerm > args.Term) {
			//do nothing except for set reply.Success as false
			//leader过期了
			//DPrintln("S7")
			reply.ConflictedIndex = -1
			reply.ConflictedTerm = -1
			reply.Success = false
		} else if (rf.currentTerm < args.Term) {
			rf.voteFor = args.LeaderID
			rf.lastVisitTime = time.Now()
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			//rf.persist()
			ConsistencyCheck(rf, args, reply)
			//DPrintln("S8")
		} else {
			reply.Success = false
			DPrintf("impossible situation!!!!!\n i am %v, %v, from leader %v\n", rf.me, rf.state, args.LeaderID)
		}
		
	}
	//DPrintf("%v(%v) receive append from %v(%v), entry = %v, sucess = %v\n", rf.me, rf.currentTerm, args.LeaderID, args.Term, args.Entries, reply.Success)
	rf.persist()
	rf.mu.Unlock()
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
// within a notTimeOut interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own notTimeOuts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//


// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }


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

// type AppendEntriesArgs struct {
//     Term int
// 	LeaderID int
// 	PrevLogIndex int
// 	PrevLogTerm int
// 	Entries     []interface{}
// 	LeaderCommit int
// }

// type AppendEntriesReply struct {
// 	Term int
// 	Success bool
//  ConflictedIndex int
//  ConflictedTerm  int
// }

func AppendCommand(rf *Raft, port *labrpc.ClientEnd, peersidx int, args *AppendEntriesArgs) (bool, bool, int) {
	
	reply := &AppendEntriesReply{}
    RPCSuccess := port.Call("Raft.AppendEntries", args, reply)
	
	rf.mu.Lock()
	//if (RPCSuccess && (reply.Term > rf.currentTerm || reply.Term > args.Term)) {
	if (RPCSuccess && (reply.Term > rf.currentTerm)) { //|| rf.currentTerm > args.Term
		rf.currentTerm = max(reply.Term, rf.currentTerm)
		rf.state = FOLLOWER
		//rf.lastVisitTime = time.Now()
		DPrintf("%v: I am no more leader!,  newterm = %v\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		return true, false, reply.Term
	}
	if (rf.currentTerm > args.Term) {
		rf.mu.Unlock()
		return true, false, reply.Term
	}
	rf.mu.Unlock()
	
	
	if (RPCSuccess && !reply.Success && reply.ConflictedIndex != -1 && reply.ConflictedTerm != -1) {  //返回失败，且不为空  conflictindex =-1 conflictterm = -1代表非冲突
		rf.mu.Lock()
		l := 0; r := len(rf.logTerm) - 1
		var mid int
		for (l < r) {
			mid = (l + r + 1) >> 1
			if (rf.logTerm[mid] <= reply.ConflictedTerm) {
				l = mid
			} else {
				r = mid - 1
			}
		}
		
		//DPrintf("l = %v, r = %v\n", l, r)
		//DPrintf("peer = %v, index = %v, term = %v, length = %v, entries = %v, confidx = %v, confterm = %v\n", 
		//peersidx, reply.ConflictedIndex, reply.ConflictedTerm, reply.Loglength, args.Entries, reply.ConflictedIndex, reply.ConflictedTerm)
		if (reply.Loglength - 1 < args.PrevLogIndex) {              //对应情况三
			//DPrintln("AppendCommand S1!")
			rf.nextIndex[peersidx] = min(reply.Loglength, rf.nextIndex[peersidx])
			DPrintf("Append failure situation 3 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
		} else if (r == -1) {
			//二分法会保证lr都在区间范围内，这里是为了预防 在设置区间时，len = 0导致r == -1的情况，也就是此时leader没有log
			DPrintf("Append failure situation 5 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
			rf.nextIndex[peersidx] = 0
		} else if (rf.logTerm[r] < reply.ConflictedTerm) {        //对应笔记情况一
			rf.nextIndex[peersidx] = min(reply.ConflictedIndex, rf.nextIndex[peersidx])
			DPrintf("Append failure situation 1 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
		} else if (rf.logTerm[r] > reply.ConflictedTerm) {         //新情况
			DPrintf("Append failure situation 4 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
			rf.nextIndex[peersidx] = 0
		} else {//rf.logTerm[l] > reply.ConflictedTerm             对应情况二
			DPrintf("Append failure situation 2 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
			rf.nextIndex[peersidx] = min(l + 1, rf.nextIndex[peersidx])
		}

		rf.mu.Unlock()


	} else if (RPCSuccess && reply.Success) {
		rf.mu.Lock()

		rf.matchIndex[peersidx] = max(rf.matchIndex[peersidx], args.PrevLogIndex + len(args.Entries))
		//DPrintf("peer %v matchidex = %v, commitedindex = %v\n", peersidx, rf.matchIndex[peersidx], rf.commitIndex)
		//rf.matchIndex[peersidx] = max(EndCommand, rf.matchIndex[peersidx])
		rf.nextIndex[peersidx] = max(len(rf.log), rf.nextIndex[peersidx])
		for i := args.PrevLogIndex + 1; i <= args.PrevLogIndex + len(args.Entries); i++ {
			if (len(rf.logAccept) - 1 <= i) {
				rf.logAccept = append(rf.logAccept, 1)
			} else {
				rf.logAccept[i]++
			}
		}
		rf.mu.Unlock()
	}
	return RPCSuccess, reply.Success, reply.Term
}

func StartSendingAppendLoop(rf *Raft, port *labrpc.ClientEnd, peersidx int, args *AppendEntriesArgs){
	ok, sucess, replyTerm := AppendCommand(rf, port, peersidx, args)
	for (ok && !sucess) {
		rf.mu.Lock()
		if (replyTerm > rf.currentTerm || args.Term != rf.currentTerm) {
			rf.state = FOLLOWER
			//rf.lastVisitTime = time.Now()
			rf.mu.Unlock()
			return
		}
		var tempPrevLogIndex, tempPrevLogTerm int
		tempPrevLogIndex = rf.nextIndex[peersidx] - 1
		if (tempPrevLogIndex == -1) {
			tempPrevLogTerm = -1
		} else {
			tempPrevLogTerm = rf.logTerm[tempPrevLogIndex]
		}

		args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, 
			PrevLogIndex: tempPrevLogIndex, PrevLogTerm: tempPrevLogTerm, LeaderCommit: rf.commitIndex}
		
		if (len(rf.log) - 1 - tempPrevLogIndex != 0) {
			args.Entries = make([]interface{}, len(rf.log) - 1 - tempPrevLogIndex)
			args.LogTerm = make([]int        , len(rf.log) - 1 - tempPrevLogIndex)
			copy(args.Entries, rf.log    [rf.nextIndex[peersidx]: len(rf.log)])
			copy(args.LogTerm, rf.logTerm[rf.nextIndex[peersidx]: len(rf.log)])
		}
		rf.mu.Unlock()
		AppendCommand(rf, port, peersidx, args)
		time.Sleep(time.Duration(20) * time.Millisecond)
	}
}

// type ApplyMsg struct {
// 	CommandValid bool
// 	Command      interface{}
// 	CommandIndex int
// }

func StartSendingAppend(rf *Raft, command interface{}, idx int) {
	rf.mu.Lock()
	for idx, port := range rf.peers {
		if (idx == rf.me) {
			continue
		}
		var tempPrevLogIndex, tempPrevLogTerm int
		tempPrevLogIndex = rf.nextIndex[idx] - 1
		if (tempPrevLogIndex == -1) {
			tempPrevLogTerm = -1
		} else {
			tempPrevLogTerm = rf.logTerm[tempPrevLogIndex]
		}

		args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, 
			PrevLogIndex: tempPrevLogIndex, PrevLogTerm: tempPrevLogTerm, LeaderCommit: rf.commitIndex}
		
		if (len(rf.log) - 1 - tempPrevLogIndex != 0) {
			args.Entries = make([]interface{}, len(rf.log) - 1 - tempPrevLogIndex)
			args.LogTerm = make([]int        , len(rf.log) - 1 - tempPrevLogIndex)
			copy(args.Entries, rf.log    [rf.nextIndex[idx]: len(rf.log)])
			copy(args.LogTerm, rf.logTerm[rf.nextIndex[idx]: len(rf.log)])
		}
		//go AppendCommand(rf, port, idx, args)

		go StartSendingAppendLoop(rf, port, idx, args )
		
	}
	rf.mu.Unlock()
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	var isLeader bool
	//var cmdIndex int
	rf.mu.Lock()


	if (rf.state == LEADER) {
		isLeader = true
	} else {
		isLeader = false
	}
    term = rf.currentTerm

	if (isLeader) {	
		//DPrintf("%v command = %v\n", rf.me, command)

		//cmdIndex = len(rf.log)
		rf.log = append(rf.log, command)
		rf.logTerm = append(rf.logTerm, rf.currentTerm)
		//DPrintf("index = %v\n", index)


		if (len(rf.logAccept) - 1 <= len(rf.log)) {
			rf.logAccept = append(rf.logAccept, 1)
		} else {
			rf.logAccept[len(rf.log)]++
		}

		DPrintf("Leader %vget command: %v, term = %v, log = %v, logterm = %v\n", rf.me, command, rf.currentTerm, rf.log, rf.logTerm)
	}
	
		
	//DPrintf("%v, %v, %v\n", rf.me, command, isLeader)
	// Your code here (2B).
	if (index == -1) {
		index = len(rf.log)
	}


	rf.persist()
	rf.mu.Unlock()
	
	//DPrintf("get start!\n")
	if (isLeader) {
		//DPrintf("i am leader %v, i am sending index = %v, term = %v, commited = %v\n", rf.me, index, term, rf.commitIndex)
		//go StartSendingAppend(rf, command, index)
	}
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


func (rf *Raft) follower() {
	rf.lastVisitTime = time.Now()
	rf.mu.Unlock()//注意在peer函数中没有释放锁，在这里释放
	for {
		time.Sleep(time.Duration(ELECTIONCHECKINTERVAL) * time.Millisecond)
		rf.mu.Lock()
		if (rf.state != FOLLOWER) {
			rf.mu.Unlock()
			return
		} else {
			if (int(time.Now().Sub(rf.lastVisitTime) / time.Millisecond) > rf.electionnotTimeOut) {//超时
				rf.state = CANDIDATE
				rf.mu.Unlock()
				return
			}
		}



		//定期检查commitidx
		for i := rf.lastApplied + 1; i <= rf.commitIndex && i <= len(rf.log) - 1; i++ {
			amsg := ApplyMsg{CommandValid: true, Command: rf.log[i], CommandIndex: i + 1}
			
			if (rf.log[i] != nil) {
				rf.applyCh <- amsg
			}
			rf.lastApplied++
			DPrintln("follower commit: ", "me: ", rf.me, " ", amsg, "log= ", rf.log, "logterm = ", rf.logTerm)
		}

		rf.mu.Unlock()
	}
}

// type AppendEntriesArgs struct {
//     Term int
// 	LeaderID int
// 	PrevLogIndex int
// 	PrevLogTerm int
// 	// entries[]
// 	// leaderCommit int
// }



// type ApplyMsg struct {
// 	CommandValid bool
// 	Command      interface{}
// 	CommandIndex int
// }

func (rf *Raft) leader() {
	//需要初始化leader的一些数据
	// termSendingAppendEntries := rf.currentTerm
	// leader := rf.me
	for i := 0; i < len(rf.peers); i++ {
        rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
	// rf.log = append(rf.log, nil)
	// rf.logTerm = append(rf.logTerm, rf.currentTerm)
	//rf.logAccept = make([]int, len(rf.log))
	rf.mu.Unlock()
    //初始化leader必须的数据结构，然后释放锁。注意在peer处上了锁，此处必须解锁
    


	for {
		rf.mu.Lock()

		if (rf.state != LEADER) {
			rf.mu.Unlock()
			//DPrintf("%v: I am no more leader!\n", rf.me)
			return
		}
		logAccept := make([]int, len(rf.log)) 
		for i := 0; i < len(rf.peers); i++ {
			if (i == rf.me) {
				continue
			}
			for j := 0; j <= rf.matchIndex[i]; j++ {
				logAccept[j]++
			}
		}
		for i := len(rf.log) - 1; i >= 0; i-- {
			if (logAccept[i] + 1 >= (len(rf.peers) + 2) / 2 && rf.logTerm[i] == rf.currentTerm) {//|| rf.logAccept[i]>= (len(rf.peers) + 2) / 2) { // && rf.logTerm[i] == rf.currentTerm
				//+2是因为要满足majority， -1是因为leader自己也算
				rf.commitIndex = i
				break
			}
		}



		//DPrintf("current commit:%v\n", rf.commitIndex)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			amsg := ApplyMsg{CommandValid: true, Command: rf.log[i], CommandIndex: i + 1}
			if (rf.log[i] != nil) {
				rf.applyCh <- amsg
			}
			rf.lastApplied++
			DPrintln("Leader commit: ", "me: ", rf.me, " ", amsg, "commitIndex = ", rf.commitIndex)// ， "log= ", rf.log, "logterm = ", rf.logTerm)
		}

		// type AppendEntriesArgs struct {
		// 	Term int
		// 	LeaderID int
		// 	PrevLogIndex int
		// 	PrevLogTerm int
		// 	Entries     []interface{}
		//  LogTerm     []int
		// 	LeaderCommit int
		// }
		
		// type AppendEntriesReply struct {
		// 	Term int
		// 	Success bool
		// 	ConflictedIndex int
		// 	ConflictedTerm  int
		// 	Loglength       int
		// 	Retry           bool   //如果retry为true，不需要修改args，直接重发
		// }
		for idx, port := range rf.peers {
			if (idx == rf.me) {
				continue
			}
			var tempPrevLogIndex, tempPrevLogTerm int
			tempPrevLogIndex = rf.nextIndex[idx] - 1
			if (tempPrevLogIndex == -1) {
				tempPrevLogTerm = -1
			} else {
				tempPrevLogTerm = rf.logTerm[tempPrevLogIndex]
			}

			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, 
				PrevLogIndex: tempPrevLogIndex, PrevLogTerm: tempPrevLogTerm, LeaderCommit: rf.commitIndex}
			
			if (len(rf.log) - 1 - tempPrevLogIndex != 0) {
				args.Entries = make([]interface{}, len(rf.log) - 1 - tempPrevLogIndex)
				args.LogTerm = make([]int        , len(rf.log) - 1 - tempPrevLogIndex)
				copy(args.Entries, rf.log    [rf.nextIndex[idx]: len(rf.log)])
				copy(args.LogTerm, rf.logTerm[rf.nextIndex[idx]: len(rf.log)])
			}
			go AppendCommand(rf, port, idx, args)
			//StartSendingAppend(rf, command, index)
			
			}
			rf.mu.Unlock()
			time.Sleep(time.Duration(HEATBEATINTERVAL) * time.Millisecond)
		}
	}



type CandidateGetVoting struct {
	mu sync.Mutex
	count int
	vote int
	cond *sync.Cond
	notTimeOut bool
}

func SendingRequest(args *RequestVoteArgs, port *labrpc.ClientEnd, candiargs *CandidateGetVoting, rf *Raft, peersidx int) {
    reply := &RequestVoteReply{}
	ok := port.Call("Raft.RequestVote", args, reply)
	candiargs.mu.Lock()
	rf.mu.Lock()
	if (ok) {
		if (rf.currentTerm < reply.Term  || reply.Term > args.Term){ //rpc成功，但是candidate状态太旧了。
			rf.currentTerm = max(reply.Term, rf.currentTerm)
			rf.state = FOLLOWER
			//rf.lastVisitTime = time.Now()
			//rf.mu.Unlock()
			//candiargs.mu.Lock()
			candiargs.notTimeOut = false
			//candiargs.mu.Unlock()
		} else if (reply.VoteGranted && rf.currentTerm == reply.Term && rf.currentTerm == args.Term) { //必须返回成功，并且给你投票的server的term必须要与你当前的term一致
			DPrintf("%v Get vote from %v, myterm = %v, voteterm=%v\n", rf.me, peersidx, rf.currentTerm, reply.Term)
			
			candiargs.vote++
			candiargs.count++
			candiargs.cond.Broadcast()
			
			
		}
	}

	rf.mu.Unlock()
	candiargs.mu.Unlock()
}

// type CandidateReturn struct {
// 	mu sync.Mutex
// 	notTimeOut bool
// }


func (rf *Raft) candidate() {
	rf.lastVisitTime = time.Now()
	rf.currentTerm++
	// DPrintln("candidate function!")
	//这里可能有大问题，先发出了requestote请求，而后再开始计时
	rf.electionnotTimeOut = getRandomElectionnotTimeOut()
	// DPrintln("notTimeOut:", rf.electionnotTimeOut)
    candiargs := &CandidateGetVoting{count: 1, vote: 1, notTimeOut: true}
	candiargs.cond = sync.NewCond(&candiargs.mu)
	termSendingVoteRequest := rf.currentTerm
	rf.voteFor = rf.me
	tempLastLogIndex := len(rf.log) - 1
	var tempLastLogTerm int
	if (tempLastLogIndex < 0) {
		tempLastLogTerm = -200
	} else {
		tempLastLogTerm = rf.logTerm[tempLastLogIndex]
	}
	rf.persist()
	rf.mu.Unlock()  //注意在peer函数中没有释放锁，在这里释放
	

	for idx, port := range rf.peers {
		if (idx == rf.me) {
			continue
		}
		args := &RequestVoteArgs{}
		args.Term = termSendingVoteRequest
		args.CandidateID = rf.me
		args.LastLogIndex = tempLastLogIndex
		args.LastLogTerm = tempLastLogTerm
		go SendingRequest(args, port, candiargs, rf, idx)
	}

	// candiReturn := &CandidateReturn{notTimeOut: true}


	go func() {
        for {
			time.Sleep(time.Duration(ELECTIONCHECKINTERVAL) * time.Millisecond)
			candiargs.mu.Lock()
			rf.mu.Lock()
			if (rf.state != CANDIDATE) {
				rf.mu.Unlock()
				candiargs.cond.Broadcast()
				candiargs.mu.Unlock()
				return

			} else if (int(time.Now().Sub(rf.lastVisitTime) / time.Millisecond) > rf.electionnotTimeOut) {
				candiargs.notTimeOut = false
				rf.mu.Unlock()
				candiargs.cond.Broadcast()
				candiargs.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			candiargs.mu.Unlock()
		}
	} ()


	totalPeer := len(rf.peers)


	candiargs.mu.Lock()
	for (candiargs.vote < (totalPeer + 2) / 2 && candiargs.count != totalPeer && candiargs.notTimeOut) {
		rf.mu.Lock()
		if (rf.state != CANDIDATE) {  //变成了follower，重新设置lastVisitedTime
			rf.mu.Unlock()
			candiargs.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		candiargs.cond.Wait()
	}


	if (candiargs.count >= (totalPeer + 2) / 2) {
		DPrintf("%v:I became Leader!term = %v, mylog = %v, logterm = %v\n",rf.me, rf.currentTerm, rf.log, rf.logTerm)
		rf.state = LEADER
		candiargs.mu.Unlock()
        return
		//需要发送heartbeat，在leader部分处理
	} else if (candiargs.count == totalPeer) {
		println("lost balloting")
		// rf.state = FOLLOWER
	} else {  //shouldreturn
		//超时了
		
	}


	candiargs.mu.Unlock()

}

func  peer(rf *Raft) {
	for {
		rf.mu.Lock()
		if (rf.killed()) {
			rf.mu.Unlock()
			return
		} else {
			//DPrintf("peer %v, state: %v, termL %v\n", rf.me, rf.state, rf.currentTerm)
			if (rf.state == FOLLOWER) {
				rf.follower()
			} else if (rf.state == LEADER) {
				rf.leader()
			} else {
				rf.candidate()
			}
		}
	}
}

func monitor(rf *Raft) {
	for {
		rf.mu.Lock()
		DPrintf("%v state == %v\n", rf.me, rf.state)
		rf.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}



func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.lastVisitTime = time.Now()
	rf.electionnotTimeOut = getRandomElectionnotTimeOut()
	rf.state = FOLLOWER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = -1
	rf.applyCh = applyCh
	rf.lastApplied = -1
	rf.voteFor = -1
	rf.currentTerm = 0
	rf.readPersist(persister.ReadRaftState())




	DPrintf("peer %v is created! currentTerm = %v, votefor = %v, log = %v\n", me, rf.currentTerm, rf.voteFor, rf.log)
    go peer(rf)
    //go monitor(rf)


	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	


	return rf
}
