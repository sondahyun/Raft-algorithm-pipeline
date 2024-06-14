package message

import (
	"encoding/gob"
	"fmt"
	"time"

	"github.com/gitferry/bamboo/config"
	"github.com/gitferry/bamboo/db"
	"github.com/gitferry/bamboo/identity"
	"github.com/gitferry/bamboo/types"
)

func init() {
	gob.Register(Transaction{})
	gob.Register(TransactionReply{})
	gob.Register(Query{})
	gob.Register(QueryReply{})
	gob.Register(Read{})
	gob.Register(ReadReply{})
	gob.Register(Register{})
	gob.Register(config.Config{})
}

/***************************
 * Client-Replica Messages *
 ***************************/

// Transaction is client reqeust with http response channel
type Transaction struct {
	Command    db.Command
	Properties map[string]string
	Timestamp  time.Time
	NodeID     identity.NodeID // forward by node
	ID         string
	C          chan TransactionReply // reply channel created by request receiver
}

// TransactionReply replies to current client session
func (r *Transaction) Reply(reply TransactionReply) {
	r.C <- reply
}

func (r Transaction) String() string {
	return fmt.Sprintf("Transaction {cmd=%v nid=%v}", r.Command, r.NodeID)
}

// TransactionReply includes all info that might replies to back the client for the coresponding reqeust
type TransactionReply struct {
	Command    db.Command
	Value      db.Value
	Properties map[string]string
	Delay      time.Duration
	Err        error
}

func NewReply(delay time.Duration) TransactionReply {
	return TransactionReply{
		Delay: delay,
	}
}

func (r TransactionReply) String() string {
	return fmt.Sprintf("TransactionReply {cmd=%v value=%x prop=%v}", r.Command, r.Value, r.Properties)
}

// Read can be used as a special request that directly read the value of key without go through replication protocol in Replica
type Read struct {
	CommandID int
	Key       db.Key
}

func (r Read) String() string {
	return fmt.Sprintf("Read {cid=%d, key=%d}", r.CommandID, r.Key)
}

// ReadReply cid and value of reading key
type ReadReply struct {
	CommandID int
	Value     db.Value
}

// Query can be used as a special request that directly read the value of key without go through replication protocol in Replica
type Query struct {
	C chan QueryReply
}

func (r *Query) Reply(reply QueryReply) {
	r.C <- reply
}

// QueryReply cid and value of reading key
type QueryReply struct {
	Info string
}

/**************************
 *     Config Related     *
 **************************/

// Register message type is used to register self (node or client) with master node
type Register struct {
	Client bool
	ID     identity.NodeID
	Addr   string
}

/************************
 *     Raft Related     *
 ************************/
type Command struct {
	Key   string
	Value int
}

type Log struct {
	Command Command    // 상태 머신에 적용될 명령
	Term    types.View // 수신된 term 번호
	Index   int
}

// 타입정의
type RequestAppendEntries struct {
	//Arguments:
	Term         types.View      // leader의 term
	LeaderID     identity.NodeID // follower가 client 요청을 redirect할 수 있는 leader의 식별자
	PrevLogIndex int             // 새로운 log entries가 추가 되기 전에, 바로 이전에 있는 log entry의 index
	PrevLogTerm  types.View      // PrevLogIndex에 해당하는 log entry의 term번호
	Entries      []Log           // 저장할 log entries들의 배열, 하트비트 메세지의 경우 배열이 비어있음
	LeaderCommit int             // 리더의 commitIndex 값, follower의 commitIndex를 업데이트하는데 사용
}

type ResponseAppendEntries struct {
	//Results:
	Term    types.View
	Success bool // true it follower contained entry matching prevLogIndex and prevLogTerm
}

type RequestVote struct {
	//Arguments:
	Term         types.View      // 요청을 보내는 candidate의 term
	CandidateID  identity.NodeID // 투표를 요청하는 candidate의 ID
	LastLogIndex int             // candidate의 마지막 log entry의 index
	LastLogTerm  types.View      // candidate의 마지막 log entry의 term
}

type ResponseVote struct {
	//Results:
	Term        types.View // candidate가 자신을 update하기 위해 사용하는 현재 term
	VoteGranted bool       // true means candidate received vote (true: 후보자가 투표받았음)
}
