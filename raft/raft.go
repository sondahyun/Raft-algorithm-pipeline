package raft

import (
	"go/types"
	"time"

	
	"github.com/gitferry/bamboo/message"
	"github.com/gitferry/bamboo/node"
)

const FORK = "fork"

type Raft struct { // 데이터 타입 정의
	// Persistent state on all servers
	CurrentTerm int        // 서버가 경험한 최신 term 번호 (초기값 0, 단조 증가)
	VotedFor    string     // 현재 term에서 투표한 candidate의 ID (없으면 null)
	Log         []LogEntry // 로그 엔트리들; 각 엔트리에는 상태 머신의 명령과 수신된 term 번호 포함 (첫 인덱스는 1)

	// Volatile state on all servers
	CommitIndex int // 커밋된 것으로 알려진 가장 높은 로그 엔트리의 인덱스 (초기값 0, 단조 증가)
	LastApplied int // 상태 머신에 적용된 가장 높은 로그 엔트리의 인덱스 (초기값 0, 단조 증가)

	// Volatile state on leaders
	NextIndex  map[string]int // 각 서버에 보낼 다음 로그 엔트리의 인덱스 (초기값 리더의 마지막 로그 인덱스 + 1)
	MatchIndex map[string]int // 각 서버에 복제된 것으로 알려진 최고 로그 엔트리의 인덱스 (초기값 0, 단조 증가)
}

type LogEntry struct {
	Command interface{} // 상태 머신에 적용될 명령
	Term    int         // 수신된 term 번호
}

// Raft 인스턴스 초기화
func NewRaft() *Raft {
	r := &Raft{
		CurrentTerm: 0,
		VotedFor:    "",             // 빈 문자열로 초기화
		Log:         []LogEntry{{}}, // LogEntry 타입의 빈 인스턴스로 초기화, 첫 인덱스가 1이 되도록 첫 번째 원소를 빈 상태로 추가
		CommitIndex: 0, //커밋되어 있는 가장 높은 log entry의 index
		LastApplied: 0, //state machine에 적용된 가장 높은 log entry의 index
		NextIndex:   make(map[string]int), // make 함수로 초기화
		MatchIndex:  make(map[string]int), // make 함수로 초기화
	}

	return r
}

func (r *Raft) StartElection() {
	electionTimer: time.Duration(ran.Intn(150)+150) * time.Millisecond
		timer := time.NewTimer(s.electionTimer)
		select {
		case <-timer.C: // Timer 만료
			node.state = types.CANDIDATE

			// 새 선거를 시작하는 로직
			// 예: r.requestVotes(node)
		//AppendEntries RPC를 받으면, timer를 다시 설정
		//appendEntriesReceived:
		//timer.Reset(electionTimeout)
		}
}
// func (r *Raft) startElection(node *node) {
// 	if node.state = types.FOLLOWER {
// 		// 현재 leader로부터 AppendEntries RPC를 받음 -> election tim
// 		electionTimer: time.Duration(ran.Intn(150)+150) * time.Millisecond
// 		timer := time.NewTimer(s.electionTimer)
// 		select {
// 		case <-timer.C: // Timer 만료
// 			node.state = types.CANDIDATE

// 			// 새 선거를 시작하는 로직
// 			// 예: r.requestVotes(node)
// 		//AppendEntries RPC를 받으면, timer를 다시 설정
// 		//appendEntriesReceived:
// 		//timer.Reset(electionTimeout)
// 		}
	
// 	}
// 	else if node.state = types.CANDIDATE {
// 		if 
// 		r.CurrentTerm++
// 		//vote for self
// 		timer.Reset(electionTimeout)
// 		r.Broadcast() //Send RequestVote to all server
		
// 		//if vote received grom majority of servers: 
// 		node.state = types.LEADER
			
// 		//if leader로부터 appendentries message를 받으면 
// 		node.state = types.FOLLOWER

// 		if case <-timer.C:
// 			//start new election
// 	}
// 	else if node.state = types.LEADER {
// 		// AppendEntreis message를 모든 server에 보냄
// 		// leader는 client로부터 받은 명령을 local log에 추가 (etnry가 state machine에 추가되면 응답)
// 		// follower의 마지막 log index >= leader의 next Index이면 leader는 해당 follower에게 next Index부터 시작하는 log entry들을 포함한 appendEntries message 전송
// 		// appendentries message가 성공하면 leader는 해당 follower의 next Index와 match Index를 업데이트
// 		// ' 로그 불일치로 실패하면 leader는 next Index를 감소시키고 재시도

// 		//commitIndex보다 큰 N이 있다면 과반수의 matchIndex[i]가 N보다 크거나 같으며, 로그[N]의 항의 임기가 현재 임기와 동일하다면, 리더는 commitIndex를 N으로 설정
// 	}


func (r *Raft) ProcessElectionLocalTmo(view types.View) {
	r.pm.AdvanceView(view)
	tmo := &pacemaker.TMO{
		View:   view + 1,
		NodeID: r.ID(),
		HighQC: r.GetHighQC(),
	}
	r.Broadcast(tmo)
	r.ProcessRemoteTmo(tmo)
}

func (r *Raft) ProcessRequestAppendEntries(msg *message.RequestAppendEntries) bool {
	if msg.Term < r.CurrentTerm {
		// 1. Reply false if term < currentTerm (§5.1)
		// follower의 현재 term이 수신된 메시지의 term보다 큰 경우, 요청 거부, false반환
		return false
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// log 일치성 검사: follower의 log에 prevLogIndex와 prevLogTerm의 entries가 없는 경우, 요청 거부, false반환
	if len(r.Log) <= msg.PrevLogIndex || r.Log[msg.PrevLogIndex].Term != msg.PrevLogTerm {
		return false
	}
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 충돌하는 entries처리: 같은 index에 서로 다른 term을 가진 entry가 존재하면, 해당 entry와 그 뒤의 모든 entry를 삭제함
	nextIndex := msg.PrevLogIndex + 1
	if len(r.Log) > nextIndex && r.Log[nextIndex].Term != msg.Term {
		r.Log = r.Log[:nextIndex] // 충돌하는 엔트리와 그 뒤 모두 제거
	}
	// 4. Append any new entries not already in the log
	// 새로운 entry추가: 아직 log에 없는 새로운 엔트리들을 추가

	r.Log = append(r.Log, msg.Entries...)
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	// commitIndex업데이트: leader의 commitIndex가 follower의 commitIndex보다 큰 경우, follower의 commitIndex를 leader의 commitIndex와 새로 추가된 마지막 엔트리의 index중 작은 값으로 설정
	if msg.LeaderCommit > r.CommitIndex {
		lastNewIndex := nextIndex + len(msg.Entries) - 1
		if msg.LeaderCommit < lastNewIndex {
			r.CommitIndex = msg.LeaderCommit
		} else {
			r.CommitIndex = lastNewIndex
		}
	}

	return true
}

func (r *Raft) ProcessRequestVote(msg *message.RequestVote) bool {// follower 입장
	// 1. Reply false if term < currentTerm (§5.1)
	// term 확인: 수신자의 현재 term이 요청받은 term보다 큰 경우, false, 투표X
	if msg.Term < r.CurrentTerm {
		return false
	}

	// 2. If votedFor is null (in Go, empty string) or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	// 수신자가 아무에게도 투표하지 않았거나, 이미 해당 candidate에게 투표한 상태,
	// candidate의 log가 수신자의 log보다 최신
	// (candidate의 마지막 log entry의 term이 수신자의 마지막 log entry의 term보다 크거나,
	// term이 같을 경우, candidate의 log entry의 index가 더 크거나 같음)이거나 같으면 투표 O
	lastLogIndex := len(r.Log) - 1
	lastLogTerm := r.Log[lastLogIndex].Term
	isLogUpToDate := msg.LastLogTerm > lastLogTerm || (msg.LastLogTerm == lastLogTerm && msg.LastLogIndex >= lastLogIndex)

	if (r.VotedFor == "" || r.VotedFor == msg.CandidateID) && isLogUpToDate {
		r.VotedFor = msg.CandidateID // 투표를 부여
		r.CurrentTerm = msg.Term     // 현재 텀을 업데이트
		return true
	}

	return false
}

func (r *Raft) ProcessResponseAppendEntries(msg *message.ResponseAppendEntries)
{
 
}

func (r *Raft) ProcessResponseVote(msg *message.ResponseVote)
{

}
//All Servers: 
//commitIndex > lastApplied: lastApplied++, log[lastApplied]를 state machine에 적용
//Request, Response의 term이 currentTerm보다 크면 currentTerm = term, 현재 상태 = follower


func (r *Raft) ProcessBlock(block *blockchain.Block) error {
	log.Debugf("[%v] is processing block from %v, view: %v, id: %x", r.ID(), block.Proposer.Node(), block.View, block.ID)
	curView := r.pm.GetCurView()
	if block.Proposer != r.ID() {
		blockIsVerified, _ := crypto.PubVerify(block.Sig, crypto.IDToByte(block.ID), block.Proposer)
		if !blockIsVerified {
			log.Warningf("[%v] received a block with an invalid signature", r.ID())
		}
	}
	if block.View > curView+1 {
		//	buffer the block
		r.bufferedBlocks[block.View-1] = block
		log.Debugf("[%v] the block is buffered, id: %x", r.ID(), block.ID)
		return nil
	}
	if block.QC != nil {
		r.updateHighQC(block.QC)
	} else {
		return fmt.Errorf("the block should contain a QC")
	}
	// does not have to process the QC if the replica is the proposer
	if block.Proposer != r.ID() {
		r.processCertificate(block.QC)
	}
	curView = r.pm.GetCurView()
	if block.View < curView {
		log.Warningf("[%v] received a stale proposal from %v", r.ID(), block.Proposer)
		return nil
	}
	if !r.Election.IsLeader(block.Proposer, block.View) {
		return fmt.Errorf("received a proposal (%v) from an invalid leader (%v)", block.View, block.Proposer)
	}
	r.bc.AddBlock(block)
	// process buffered QC
	qc, ok := r.bufferedQCs[block.ID]
	if ok {
		r.processCertificate(qc)
		delete(r.bufferedQCs, block.ID)
	}

	shouldVote, err := r.votingRule(block)
	if err != nil {
		log.Errorf("[%v] cannot decide whether to vote the block, %w", r.ID(), err)
		return err
	}
	if !shouldVote {
		log.Debugf("[%v] is not going to vote for block, id: %x", r.ID(), block.ID)
		return nil
	}
	vote := blockchain.MakeVote(block.View, r.ID(), block.ID)
	// vote is sent to the next leader
	voteAggregator := r.FindLeaderFor(block.View + 1)
	if voteAggregator == r.ID() {
		log.Debugf("[%v] vote is sent to itself, id: %x", r.ID(), vote.BlockID)
		r.ProcessVote(vote)
	} else {
		log.Debugf("[%v] vote is sent to %v, id: %x", r.ID(), voteAggregator, vote.BlockID)
		r.Send(voteAggregator, vote)
	}
	b, ok := r.bufferedBlocks[block.View]
	if ok {
		_ = r.ProcessBlock(b)
		delete(r.bufferedBlocks, block.View)
	}
	return nil
}

func (r *HotStuff) ProcessVote(vote *blockchain.Vote) {
	log.Debugf("[%v] is processing the vote, block id: %x", r.ID(), vote.BlockID)
	if vote.Voter != r.ID() {
		voteIsVerified, err := crypto.PubVerify(vote.Signature, crypto.IDToByte(vote.BlockID), vote.Voter)
		if err != nil {
			log.Warningf("[%v] Error in verifying the signature in vote id: %x", r.ID(), vote.BlockID)
			return
		}
		if !voteIsVerified {
			log.Warningf("[%v] received a vote with invalid signature. vote id: %x", r.ID(), vote.BlockID)
			return
		}
	}
	isBuilt, qc := r.bc.AddVote(vote)
	if !isBuilt {
		log.Debugf("[%v] not sufficient votes to build a QC, block id: %x", r.ID(), vote.BlockID)
		return
	}
	qc.Leader = r.ID()
	// buffer the QC if the block has not been received
	_, err := r.bc.GetBlockByID(qc.BlockID)
	if err != nil {
		r.bufferedQCs[qc.BlockID] = qc
		return
	}
	r.processCertificate(qc)
}

func (r *HotStuff) ProcessRemoteTmo(tmo *pacemaker.TMO) {
	log.Debugf("[%v] is processing tmo from %v", r.ID(), tmo.NodeID)
	r.processCertificate(tmo.HighQC)
	isBuilt, tc := r.pm.ProcessRemoteTmo(tmo)
	if !isBuilt {
		return
	}
	log.Debugf("[%v] a tc is built for view %v", r.ID(), tc.View)
	r.processTC(tc)
}

func (r *Raft) MakeProposal(view types.View, payload []*message.Transaction) *blockchain.Block {
	qc := r.forkChoice()
	block := blockchain.MakeBlock(view, qc, qc.BlockID, payload, r.ID())
	return block
}

func (r *HotStuff) GetChainStatus() string {
	chainGrowthRate := r.bc.GetChainGrowth()
	blockIntervals := r.bc.GetBlockIntervals()
	return fmt.Sprintf("[%v] The current view is: %v, chain growth rate is: %v, ave block interval is: %v", r.ID(), r.pm.GetCurView(), chainGrowthRate, blockIntervals)
}