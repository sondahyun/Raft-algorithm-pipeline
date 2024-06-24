package raft

import (
	"encoding/gob"
	"fmt"
	"math/rand"

	"time"

	"go.uber.org/atomic"

	"github.com/gitferry/bamboo/blockchain"
	"github.com/gitferry/bamboo/config"
	"github.com/gitferry/bamboo/election"
	"github.com/gitferry/bamboo/identity"
	"github.com/gitferry/bamboo/log"
	"github.com/gitferry/bamboo/mempool"
	"github.com/gitferry/bamboo/message"
	"github.com/gitferry/bamboo/node"
	"github.com/gitferry/bamboo/pacemaker"
	"github.com/gitferry/bamboo/types"
)

type Replica struct {
	node.Node
	RaftSafety
	election.Election
	config.Config

	table     map[string]int
	checkVote map[types.View]bool

	pd            *mempool.Producer
	pm            *pacemaker.Pacemaker
	start         chan bool // signal to start the node
	isStarted     atomic.Bool
	isByz         bool
	heartbeat     *time.Timer // timeout for each view
	electionTimer *time.Timer // timeout for each view

	// Persistent state on all servers
	CurrentTerm types.View         // 서버가 경험한 최신 term 번호 (초기값 0, 단조 증가)
	VotedFor    identity.NodeID    // 현재 term에서 투표한 candidate의 ID (없으면 null)
	LogEntry    []message.Log      // 로그 엔트리들; 각 엔트리에는 상태 머신의 명령과 수신된 term 번호 포함 (첫 인덱스는 1)
	VoteNum     map[types.View]int //vote 수
	SuccessNum  map[types.View]int //ResponseAppendEntries 수
	TotalNum    int

	// Volatile state on all servers
	CommitIndex int // 커밋된 것으로 알려진 가장 높은 로그 엔트리의 인덱스 (초기값 0, 단조 증가)
	LastApplied int // 상태 머신에 적용된 가장 높은 로그 엔트리의 인덱스 (초기값 0, 단조 증가)

	// Volatile state on leaders
	NextIndex  map[message.Log]int // 각 서버에 보낼 다음 로그 엔트리의 인덱스 (초기값 리더의 마지막 로그 인덱스 + 1)
	MatchIndex map[message.Log]int // 각 서버에 복제된 것으로 알려진 최고 로그 엔트리의 인덱스 (초기값 0, 단조 증가)

	committedBlocks chan *blockchain.Block
	forkedBlocks    chan *blockchain.Block
	eventChan       chan interface{}

	/* for monitoring node statistics */
	thrus                string
	lastViewTime         time.Time
	startTime            time.Time
	tmpTime              time.Time
	voteStart            time.Time
	totalCreateDuration  time.Duration
	totalProcessDuration time.Duration
	totalProposeDuration time.Duration
	totalDelay           time.Duration
	totalRoundTime       time.Duration
	totalVoteTime        time.Duration
	totalBlockSize       int
	receivedNo           int
	roundNo              int
	voteNo               int //vote 수
	totalCommittedTx     int
	latencyNo            int
	proposedNo           int
	processedNo          int
	committedNo          int
}

// NewReplica creates a new replica instance
func NewRaftReplica(id identity.NodeID, alg string, isByz bool) *Replica {
	r := new(Replica)
	r.Node = node.NewNode(id, isByz)
	if isByz {
		log.Infof("[%v] is Byzantine", r.ID())
	}
	r.Election = election.NewRaftElection() //static
	r.isByz = isByz
	r.pd = mempool.NewProducer()
	r.pm = pacemaker.NewPacemaker(config.GetConfig().N())
	r.start = make(chan bool)
	r.eventChan = make(chan interface{})
	r.committedBlocks = make(chan *blockchain.Block, 100)
	r.forkedBlocks = make(chan *blockchain.Block, 100)
	r.Register(blockchain.Block{}, r.HandleBlock)
	r.Register(blockchain.Vote{}, r.HandleVote)
	r.Register(pacemaker.TMO{}, r.HandleTmo)

	//초기화
	r.table = make(map[string]int)
	r.checkVote = make(map[types.View]bool)
	r.CurrentTerm = types.View(0)
	r.VotedFor = "" // 빈 문자열로 초기화
	r.VoteNum = make(map[types.View]int)
	r.TotalNum = config.GetConfig().N()
	r.LogEntry = []message.Log{{}}           // message.LogEntry 타입의 빈 인스턴스로 초기화, 첫 인덱스가 1이 되도록 첫 번째 원소를 빈 상태로 추가
	r.CommitIndex = 0                        //커밋되어 있는 가장 높은 log entry의 index
	r.LastApplied = 0                        //state machine에 적용된 가장 높은 log entry의 index
	r.NextIndex = make(map[message.Log]int)  // make 함수로 초기화
	r.MatchIndex = make(map[message.Log]int) // make 함수로 초기화
	r.SuccessNum = make(map[types.View]int)

	r.Register(message.Transaction{}, r.handleTxn)
	r.Register(message.Query{}, r.handleQuery)
	r.Register(message.RequestAppendEntries{}, r.handleRequestAppendEntries)
	r.Register(message.ResponseAppendEntries{}, r.handleResponseAppendEntries)
	r.Register(message.CommitAppendEntries{}, r.handleCommitAppendEntries)
	r.Register(message.RequestVote{}, r.handleRequestVote)
	r.Register(message.ResponseVote{}, r.handleResponseVote)

	gob.Register(blockchain.Block{})
	gob.Register(blockchain.Vote{})
	gob.Register(pacemaker.TC{})
	gob.Register(pacemaker.TMO{})
	gob.Register(message.RequestAppendEntries{})
	gob.Register(message.ResponseAppendEntries{})
	gob.Register(message.CommitAppendEntries{})
	gob.Register(message.RequestVote{})
	gob.Register(message.ResponseVote{})

	// Is there a better way to reduce the number of parameters?
	switch alg {
	// case "Raft":
	// 	r.RaftSafety = Raft.NewRaft(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	// case "tchs":
	// 	r.RaftSafety = tchs.NewTchs(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	// case "streamlet":
	// 	r.RaftSafety = streamlet.NewStreamlet(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	// case "lbft":
	// 	r.RaftSafety = lbft.NewLbft(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	// case "fastRaft":
	// 	r.RaftSafety = fhs.NewFhs(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	default:
		r.RaftSafety = NewRaft(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	}
	return r
}

/* Message Handlers */ //메세지 핸들러
func (r *Replica) handleRequestAppendEntries(msg message.RequestAppendEntries) {
	r.eventChan <- msg
}

func (r *Replica) handleResponseAppendEntries(msg message.ResponseAppendEntries) {
	r.eventChan <- msg
}

func (r *Replica) handleCommitAppendEntries(msg message.CommitAppendEntries) {
	r.eventChan <- msg
}

func (r *Replica) handleRequestVote(msg message.RequestVote) {
	r.eventChan <- msg
}

func (r *Replica) handleResponseVote(msg message.ResponseVote) {
	r.eventChan <- msg
}

func (r *Replica) HandleBlock(block blockchain.Block) {
	r.receivedNo++
	r.startSignal()
	log.Debugf("[%v] received a block from %v, view is %v, id: %x, prevID: %x", r.ID(), block.Proposer, block.View, block.ID, block.PrevID)
	r.eventChan <- block
}

func (r *Replica) HandleVote(vote blockchain.Vote) {
	if vote.View < r.pm.GetCurView() {
		return
	}
	r.startSignal()
	log.Debugf("[%v] received a vote frm %v, blockID is %x", r.ID(), vote.Voter, vote.BlockID)
	r.eventChan <- vote
}

func (r *Replica) HandleTmo(tmo pacemaker.TMO) {
	if tmo.View < r.pm.GetCurView() {
		return
	}
	log.Debugf("[%v] received a timeout from %v for view %v", r.ID(), tmo.NodeID, tmo.View)
	r.eventChan <- tmo
}

// handleQuery replies a query with the statistics of the node
func (r *Replica) handleQuery(m message.Query) {
	//realAveProposeTime := float64(r.totalProposeDuration.Milliseconds()) / float64(r.processedNo)
	//aveProcessTime := float64(r.totalProcessDuration.Milliseconds()) / float64(r.processedNo)
	//aveVoteProcessTime := float64(r.totalVoteTime.Milliseconds()) / float64(r.roundNo)
	//aveBlockSize := float64(r.totalBlockSize) / float64(r.proposedNo)
	//requestRate := float64(r.pd.TotalReceivedTxNo()) / time.Now().Sub(r.startTime).Seconds()
	//committedRate := float64(r.committedNo) / time.Now().Sub(r.startTime).Seconds()
	//aveRoundTime := float64(r.totalRoundTime.Milliseconds()) / float64(r.roundNo)
	//aveProposeTime := aveRoundTime - aveProcessTime - aveVoteProcessTime
	latency := float64(r.totalDelay.Milliseconds()) / float64(r.latencyNo)
	r.thrus += fmt.Sprintf("Time: %v s. Throughput: %v txs/s\n", time.Now().Sub(r.startTime).Seconds(), float64(r.totalCommittedTx)/time.Now().Sub(r.tmpTime).Seconds())
	r.totalCommittedTx = 0
	r.tmpTime = time.Now()
	status := fmt.Sprintf("Latency: %v\n%s", latency, r.thrus)
	//status := fmt.Sprintf("chain status is: %s\nCommitted rate is %v.\nAve. block size is %v.\nAve. trans. delay is %v ms.\nAve. creation time is %f ms.\nAve. processing time is %v ms.\nAve. vote time is %v ms.\nRequest rate is %f txs/s.\nAve. round time is %f ms.\nLatency is %f ms.\nThroughput is %f txs/s.\n", r.Safety.GetChainStatus(), committedRate, aveBlockSize, aveTransDelay, aveCreateDuration, aveProcessTime, aveVoteProcessTime, requestRate, aveRoundTime, latency, throughput)
	//status := fmt.Sprintf("Ave. actual proposing time is %v ms.\nAve. proposing time is %v ms.\nAve. processing time is %v ms.\nAve. vote time is %v ms.\nAve. block size is %v.\nAve. round time is %v ms.\nLatency is %v ms.\n", realAveProposeTime, aveProposeTime, aveProcessTime, aveVoteProcessTime, aveBlockSize, aveRoundTime, latency)
	m.Reply(message.QueryReply{Info: status})
}

func (r *Replica) handleTxn(m message.Transaction) {
	r.pd.AddTxn(&m)
	r.startSignal()
	// the first leader kicks off the protocol
	if r.pm.GetCurView() == 0 && r.IsLeader(r.ID(), 1) {
		log.Debugf("[%v] is going to kick off the protocol", r.ID())
		r.pm.AdvanceView(0)
	}
}

/* Processors */

func (r *Replica) processCommittedBlock(block *blockchain.Block) {
	if block.Proposer == r.ID() {
		for _, txn := range block.Payload {
			// only record the delay of transactions from the local memory pool
			delay := time.Now().Sub(txn.Timestamp)
			r.totalDelay += delay
			r.latencyNo++
		}
	}
	r.committedNo++
	r.totalCommittedTx += len(block.Payload)
	log.Infof("[%v] the block is committed, No. of transactions: %v, view: %v, current view: %v, id: %x", r.ID(), len(block.Payload), block.View, r.pm.GetCurView(), block.ID)
}

func (r *Replica) processForkedBlock(block *blockchain.Block) {
	if block.Proposer == r.ID() {
		for _, txn := range block.Payload {
			// collect txn back to mem pool
			r.pd.CollectTxn(txn)
		}
	}
	log.Infof("[%v] the block is forked, No. of transactions: %v, view: %v, current view: %v, id: %x", r.ID(), len(block.Payload), block.View, r.pm.GetCurView(), block.ID)
}

func (r *Replica) processNewView(newView types.View) {
	log.Debugf("[%v] is processing new view: %v, leader is %v", r.ID(), newView, r.FindLeaderFor(newView))
	if !r.IsLeader(r.ID(), newView) {
		return
	}
	r.proposeBlock(newView)
}

func (r *Replica) proposeBlock(view types.View) {
	createStart := time.Now()
	block := r.RaftSafety.MakeProposal(view, r.pd.GeneratePayload())
	r.totalBlockSize += len(block.Payload)
	r.proposedNo++
	createEnd := time.Now()
	createDuration := createEnd.Sub(createStart)
	block.Timestamp = time.Now()
	r.totalCreateDuration += createDuration
	r.Node.Broadcast(block)
	_ = r.RaftSafety.ProcessBlock(block)
	r.voteStart = time.Now()
}

func (r *Replica) ProcessLog() { //leader

	txs := r.pd.GeneratePayload()
	tx := txs[0]

	AddLog := message.Log{
		Command: &message.Command{
			Key:   tx.Key,
			Value: tx.Value,
		},
		Term: r.CurrentTerm,
	}
	msg := message.RequestAppendEntries{
		Term:         r.CurrentTerm,
		LeaderID:     r.ID(),
		PrevLogIndex: len(r.LogEntry) - 1,
		PrevLogTerm:  r.CurrentTerm - 1,
		Entries:      AddLog,
		LeaderCommit: 0,
	}

	r.Broadcast(msg)

	log.Debugf("[%v] Leader Broadcast Real RequestAppendEntries", r.ID())

}

// ListenCommittedBlocks listens committed blocks and forked blocks from the protocols
func (r *Replica) ListenCommittedBlocks() {
	for {
		select {
		case committedBlock := <-r.committedBlocks:
			r.processCommittedBlock(committedBlock)
		case forkedBlock := <-r.forkedBlocks:
			r.processForkedBlock(forkedBlock)
		}
	}
}

func (r *Replica) startSignal() {
	if !r.isStarted.Load() {
		r.startTime = time.Now()
		r.tmpTime = time.Now()
		log.Debugf("[%v] is boosting", r.ID())
		r.isStarted.Store(true)
		r.start <- true
	}
}

func (r *Replica) startElectionTimer() {
	log.Debugf("[%v]start startElectionTimer", r.ID())
	//seed (현재시간 기준)
	rand.New(rand.NewSource(time.Now().UnixNano()))

	//rand.Seed(time.Now().UnixNano())

	// 0~99까지의 난수 생성
	randomNumber := rand.Intn(100) + 100
	r.electionTimer = time.NewTimer(time.Duration(randomNumber) * time.Millisecond)

	// 여기서 타이머 기다리는 중

	<-r.electionTimer.C
	log.Debugf("[%v] Election timer TMO", r.ID())

	r.SetState(types.CANDIDATE)
	r.CurrentTerm++

	msg := message.RequestVote{
		Term:         r.CurrentTerm,
		CandidateID:  r.Node.ID(),
		LastLogIndex: r.CommitIndex,                  //candidate의 마지막 log entry의 index
		LastLogTerm:  r.LogEntry[r.CommitIndex].Term, //candidate의 마지막 log entry의 term
	}

	// vote for self
	r.VoteNum[r.CurrentTerm]++
	log.Debugf("[%v]vote mySelf", r.ID())

	// send RequestVote message to all server
	r.Broadcast(msg)
	log.Debugf("[%v] CurrentTerm: [%v]", r.ID(), r.CurrentTerm)

	log.Debugf("[%v]finish startElectionTimer", r.ID())

	// for {
	// 	select {
	// 	case event := <-r.eventChan: // r.eventChan에서 메시지를 받았을 경우
	// 		switch msg := event.(type) {
	// 		case message.RequestAppendEntries:
	// 			//if) new leader로부터 AppendEntries를 받음: 다시 follower로 전환
	// 			log.Debugf("[%v]Received AppendEntries from %v", r.ID(), msg.LeaderID)
	// 			// state = follower
	// 			r.SetState(types.FOLLOWER)
	// 			// timer reset
	// 			r.electionTimer.Reset(time.Duration(randomNumber) * time.Millisecond)
	// 		}
	// 	case <-r.electionTimer.C: //r.electionTimer.C channel로부터 메시지를 받을 때(타이머 만료)까지 대기(block)
	// 		r.SetState(types.CANDIDATE)
	// 		r.CurrentTerm++

	// 		msg := message.RequestVote{
	// 			Term:         r.CurrentTerm,
	// 			CandidateID:  r.Node.ID(),
	// 			LastLogIndex: r.CommitIndex,                  //candidate의 마지막 log entry의 index
	// 			LastLogTerm:  r.LogEntry[r.CommitIndex].Term, //candidate의 마지막 log entry의 term
	// 		}
	// 		// vote for self
	// 		r.VoteNum[r.CurrentTerm]++
	// 		log.Debugf("[%v]vote mySelf", r.ID())

	// 		// send RequestVote message to all server
	// 		r.Broadcast(msg)
	// 		log.Debugf("[%v]finish startElectionTimer", r.ID())

	// 	}
	// }
}

// startHeartbeatTimer listens new view and timeout events
// heartbeat Timer
func (r *Replica) startHeartbeatTimer() { //heartbeat timer돌다가 electiontimeout되면 heartbeat멈춤
	//리더가 heartbeat timer맞춰서 appendentries message보냄 (broadcast)
	log.Debugf("[%v] leader start heartbeatTimer", r.ID())
	log.Debugf("[%v] CurrentTerm: [%v]", r.ID(), r.CurrentTerm)

	for r.GetState() == types.LEADER && !r.IsFault() {
		randomNumber := rand.Intn(100) + 100
		r.heartbeat = time.NewTimer(time.Duration(randomNumber/2) * time.Millisecond)

		<-r.heartbeat.C

		AddLog := message.Log{
			Command: &message.Command{
				Key:   "",
				Value: 0,
			},
			Term: r.CurrentTerm,
		}
		log.Debugf("[%v]leader send RequestAppendEntries", r.ID())
		msg := message.RequestAppendEntries{
			Term:         r.CurrentTerm,
			LeaderID:     r.ID(),
			PrevLogIndex: 0,
			PrevLogTerm:  r.CurrentTerm,
			Entries:      AddLog,
			LeaderCommit: 0,
		}
		r.Broadcast(msg)
		log.Debugf("[%v] Leader Broadcast Empty RequestAppendEntries", r.ID())
	}
	r.SetState(types.FOLLOWER)
}

func (r *Replica) hearbeatTMOtest() {
	<-r.heartbeat.C
	r.heartbeat = nil
	log.Debugf("[%v] Heartbeat timer TMO", r.ID())

	go r.startElectionTimer()
	// for {
	// 	select {
	// 	case <-r.heartbeat.C: //heartbeat TMO
	// 		go r.startElectionTimer() //leader election Timer 시작
	// 		return
	// 	}
	// }
}

// Start starts event loop
func (r *Replica) Start() {
	go r.Run()
	// wait for the start signal
	<-r.start
	log.Debugf("[%v] node start", r.ID())
	log.Debugf("[%v] CurrentTerm: [%v]", r.ID(), r.CurrentTerm)

	go r.startElectionTimer()    // startElectionTimer
	go r.ListenCommittedBlocks() // ListenCommittedBlocks listens committed blocks and forked blocks from the protocols

	for r.isStarted.Load() {
		event := <-r.eventChan
		// r.timer.Reset()
		switch v := event.(type) {
		// case types.View:
		// 	r.processNewView(v)
		case pacemaker.TMO:
			// r.RaftSafety.ProcessRemoteTmo(&v)
		case message.RequestAppendEntries: //Follwer
			//r.RaftSafety.ProcessRequestAppendEntries(&v)
			//heartbeat reset
			// HeartBeat 없으면 생성과 동시에 hearbeatTMOtest 함수 실행
			log.Debugf("[%v] follower가 RequestAppendEntries 받음", r.ID())

			if r.heartbeat == nil {
				log.Debugf("[%v] follower start heartbeatTimer", r.ID())

				randomNumber := rand.Intn(100) + 100
				r.heartbeat = time.NewTimer(time.Duration(randomNumber) * time.Millisecond)
				log.Debugf("[%v] randomNumber", randomNumber)

				go r.hearbeatTMOtest()
			}
			randomNumber := rand.Intn(100) + 100
			r.heartbeat.Reset(time.Duration(randomNumber) * time.Millisecond)
			go r.hearbeatTMOtest()

			//Add AppendEntries (Entries != empty)
			if v.Entries.Command.Key != "" {
				r.table[v.Entries.Command.Key] = v.Entries.Command.Value

				if v.Term < r.CurrentTerm {
					//log.Debug("이상해씨")
					continue
				}
				if v.Term > r.CurrentTerm {
					r.CurrentTerm = v.Term
				}

				// // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
				// if len(r.LogEntry) < v.PrevLogIndex || r.LogEntry[v.PrevLogIndex].Term != v.PrevLogTerm {
				// 	log.Debugf("[%v]피카츄", r.ID())
				// 	continue
				// } //질문: LogEntry == PrevLogIndex ? 414도

				// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
				//Entries의 index
				// 기존 항목이 새 항목과 충돌하는 경우(인덱스는 같지만 용어가 다른 경우), 기존 항목과 그 뒤에 오는 모든 항목을 삭제합니다.
				if v.PrevLogIndex > 0 {
					if r.LogEntry[v.PrevLogIndex+1].Term != v.Entries.Term {
						r.LogEntry = r.LogEntry[:v.PrevLogIndex+1]
						log.Debugf("[%v]기존 항목이 새 항목과 충돌", r.ID())
					}
				}

				// // 4. Append any new entries not already in the log
				// r.LogEntry[v.PrevLogIndex+1].Command = v.Entries[v.PrevLogIndex+1].Command
				// r.LogEntry[v.PrevLogIndex+1].Term = v.Entries[v.PrevLogIndex+1].Term
				// log.Debugf("[%v]피카츄2", r.ID())

				// // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
				// if v.LeaderCommit > r.CommitIndex {
				// 	if v.LeaderCommit <= v.PrevLogIndex {
				// 		r.CommitIndex = (v.LeaderCommit)
				// 	}
				// 	r.CommitIndex = (v.PrevLogIndex)
				// }
				// log.Debugf("[%v]피카츄3", r.ID())

				// AddLog := message.Log{
				// 	Command: message.Command{
				// 		Key:   v.Key,
				// 		Value: v.Value,
				// 	},
				// 	Term:  r.CurrentTerm,
				// 	Index: 0, // 인덱스 1로 설정
				// }

				// r.LogEntry = append(r.LogEntry, AddLog)

				// log.Debugf("[%v] LogEntry: [%v] <- [%v], Index: [%+v]", r.ID(), v.Key, v.Value, len(r.LogEntry)-1)
				// //log.Debugf("[%v] LogEntry All: %+v", r.ID(), r.LogEntry)
				// // 원하는 형태로 LogEntry 배열 출력
				// logEntriesStr := fmt.Sprintf("[%v] currentTerm: [%v], LogEntries[%d] ->", r.ID(), r.CurrentTerm, len(r.LogEntry)-1)
				// for i, logEntry := range r.LogEntry {
				// 	if i == 0 {
				// 		continue
				// 	}
				// 	logEntriesStr += fmt.Sprintf(" [%s<=%d]", logEntry.Command.Key, logEntry.Command.Value)
				// }
				// log.Debugf(logEntriesStr)

				msg := message.ResponseAppendEntries{
					Term:    r.CurrentTerm,
					Success: true,
					Entries: v.Entries,
				}
				r.Send(identity.NodeID(v.LeaderID), msg)

				log.Debugf("[%v] follower가 real RequestAppendEntries 처리 완료", r.ID())
			}
			if v.Entries.Command.Key == "" { //질문: heartbeat append entries수신후 responseAppendEntreis에 msg 보내야되는지
				// msg := message.ResponseAppendEntries{
				// 	Term:    r.CurrentTerm,
				// 	Success: true,
				// 	Entries: v.Entries,
				// }
				// r.Send(identity.NodeID(v.LeaderID), msg)

				log.Debugf("[%v] follower가 empty RequestAppendEntries 처리 완료", r.ID())

			}
			log.Debugf("[%v] follower가 ResponseAppendEntries send", r.ID())

		case message.ResponseAppendEntries: //leader
			if v.Term > r.CurrentTerm {
				r.CurrentTerm = v.Term
			}
			if v.Success {
				r.SuccessNum[v.Term]++
			}
			if r.SuccessNum[v.Term] <= r.TotalNum/2 { //모든 follower가 success하지 않으면 continue
				continue
			}
			msg := message.CommitAppendEntries{
				Term:    r.CurrentTerm,
				Entries: v.Entries,
			}
			r.Broadcast(msg)
			r.LogEntry = append(r.LogEntry, v.Entries)
			log.Debugf("[%v] leader가 LogRepli 정족수 확인 완료", r.ID())
			log.Debugf("[%v] leader가 LogReplication 완료하고 CommitAppendEntreis broadcast", r.ID())

			//leader가 commit
			//client에 값 전달
		case message.CommitAppendEntries:
			r.LogEntry = append(r.LogEntry, v.Entries)
			log.Debugf("[%v]가 CommitAppendEntries 처리 완료", r.ID())
			log.Debugf("[%v] follower가 LogReplication 완료", r.ID())

			//log.Debugf("[%v] LogEntry: [%v] <- [%v], Index: [%+v]", r.ID(), v.Key, v.Value, len(r.LogEntry)-1)
			//log.Debugf("[%v] LogEntry All: %+v", r.ID(), r.LogEntry)
			// 원하는 형태로 LogEntry 배열 출력
			logEntriesStr := fmt.Sprintf("[%v] v.Term: [%v], currentTerm: [%v], LogEntries[%d] ->", r.ID(), v.Term, r.CurrentTerm, len(r.LogEntry)-1)
			for i, logEntry := range r.LogEntry {
				if i == 0 {
					continue
				}
				logEntriesStr += fmt.Sprintf(" [%s<=%d]", logEntry.Command.Key, logEntry.Command.Value)
			}
			log.Debugf(logEntriesStr)

		case message.RequestVote:
			log.Debugf("[%v]가 ReqeustVote받음", r.ID())

			// leader, candidate pass
			if r.GetState() != types.FOLLOWER {
				continue
			}
			if v.Term < r.CurrentTerm {
				continue
			}
			if v.Term > r.CurrentTerm {
				r.CurrentTerm = v.Term
			}
			// follower
			r.electionTimer.Stop() // follower가 candidate가 되는 것을 막는 로직
			log.Debugf("[%v] follower가 electionTimer Stop", r.ID())

			// Request확인, vote to candidate
			msg := message.ResponseVote{
				Term:        r.CurrentTerm,
				VoteGranted: true,
			}
			r.Send(v.CandidateID, msg)
			log.Debugf("[%v] CurrentTerm: [%v]", r.ID(), r.CurrentTerm)

			log.Debugf("[%v] follower가 Send ResponseVote", r.ID())

		case message.ResponseVote:
			// 받은 투표를 확인해서 정족수에 충족하면 리더가 됨
			if v.Term > r.CurrentTerm {
				r.CurrentTerm = v.Term
			}
			if v.VoteGranted {
				r.VoteNum[v.Term]++
			}
			if r.VoteNum[v.Term] < r.TotalNum { //quorum 만족X
				continue
			}
			// if r.VoteNum[v.Term] <= r.TotalNum/2 { //quorum 만족X
			// 	continue
			// }

			log.Debugf("[%v] Receive ResponseVote, 정족수 확인 완료", r.ID())
			log.Debugf("[%v] CurrentTerm: [%v]", r.ID(), r.CurrentTerm)

			r.SetState(types.LEADER)
			log.Debugf("[%v]가 leader", r.ID())

			go r.startHeartbeatTimer()
			r.ProcessLog()

			// 클라이언트로 부터 받은 값으로 합의 시작
			//r.RaftSafety.ProcessResponseVote(&v)
		}
	}
}
