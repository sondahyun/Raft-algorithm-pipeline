package raft

import (
	"github.com/gitferry/bamboo/log"
	"github.com/gitferry/bamboo/message"
	"github.com/gitferry/bamboo/node"
	"github.com/gitferry/bamboo/pacemaker"
	"github.com/gitferry/bamboo/types"
)

const FORK = "fork"

type Raft struct { // 데이터 타입 정의
	node.Node
	pm *pacemaker.Pacemaker
}

// Raft 인스턴스 초기화
func NewRaft(node node.Node, pm *pacemaker.Pacemaker) *Raft {
	r := &Raft{
		Node:        node,
		CurrentTerm: types.View(0),
		VotedFor:    "",                     // 빈 문자열로 초기화
		Log:         []message.LogEntry{{}}, // message.LogEntry 타입의 빈 인스턴스로 초기화, 첫 인덱스가 1이 되도록 첫 번째 원소를 빈 상태로 추가
		CommitIndex: 0,                      //커밋되어 있는 가장 높은 log entry의 index
		LastApplied: 0,                      //state machine에 적용된 가장 높은 log entry의 index
		NextIndex:   make(map[string]int),   // make 함수로 초기화
		MatchIndex:  make(map[string]int),   // make 함수로 초기화
	}

	return r
}

func (r *Raft) ProcessElectionLocalTmo(view types.View) {

}

// func (r *Raft) ProcessRequestAppendEntries(msg *message.RequestAppendEntries) bool {
// 	log.Infof("[%v]start ProcessRequestAppendEntries", r.ID())

// 	if msg.Term < r.CurrentTerm {
// 		// 1. Reply false if term < currentTerm (§5.1)
// 		// follower의 현재 term이 수신된 메시지의 term보다 큰 경우, 요청 거부, false반환
// 		log.Errorf("s")
// 		return false
// 	}

// 	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
// 	// whose term matches prevLogTerm (§5.3)
// 	// log 일치성 검사: follower의 log에 prevLogIndex와 prevLogTerm의 entries가 없는 경우, 요청 거부, false반환
// 	if len(r.Log) <= msg.PrevLogIndex || r.Log[msg.PrevLogIndex].Term != msg.PrevLogTerm {
// 		return false
// 	}
// 	// 3. If an existing entry conflicts with a new one (same index
// 	// but different terms), delete the existing entry and all that
// 	// follow it (§5.3)
// 	// 충돌하는 entries처리: 같은 index에 서로 다른 term을 가진 entry가 존재하면, 해당 entry와 그 뒤의 모든 entry를 삭제함
// 	nextIndex := msg.PrevLogIndex + 1
// 	if len(r.Log) > nextIndex && r.Log[nextIndex].Term != msg.Term {
// 		r.Log = r.Log[:nextIndex] // 충돌하는 엔트리와 그 뒤 모두 제거
// 	}
// 	// 4. Append any new entries not already in the log
// 	// 새로운 entry추가: 아직 log에 없는 새로운 엔트리들을 추가

// 	r.Log = append(r.Log, msg.Entries...)
// 	// 5. If leaderCommit > commitIndex, set commitIndex =
// 	// min(leaderCommit, index of last new entry)
// 	// commitIndex업데이트: leader의 commitIndex가 follower의 commitIndex보다 큰 경우, follower의 commitIndex를 leader의 commitIndex와 새로 추가된 마지막 엔트리의 index중 작은 값으로 설정
// 	if msg.LeaderCommit > r.CommitIndex {
// 		lastNewIndex := nextIndex + len(msg.Entries) - 1
// 		if msg.LeaderCommit < lastNewIndex {
// 			r.CommitIndex = msg.LeaderCommit
// 		} else {
// 			r.CommitIndex = lastNewIndex
// 		}
// 	}

// 	log.Info("finish ProcessRequestAppendEntries")

// 	return true
// }

// func (r *Raft) ProcessRequestVote(msg *message.RequestVote) bool { // follower 입장
// 	// 1. Reply false if term < currentTerm (§5.1)
// 	// term 확인: 수신자의 현재 term이 요청받은 term보다 큰 경우, false, 투표X
// 	// if msg.Term < r.CurrentTerm {
// 	// 	return false
// 	// }

// 	// // 2. If votedFor is null (in Go, empty string) or candidateId, and candidate’s log is at
// 	// // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
// 	// // 수신자가 아무에게도 투표하지 않았거나, 이미 해당 candidate에게 투표한 상태,
// 	// // candidate의 log가 수신자의 log보다 최신
// 	// // (candidate의 마지막 log entry의 term이 수신자의 마지막 log entry의 term보다 크거나,
// 	// // term이 같을 경우, candidate의 log entry의 index가 더 크거나 같음)이거나 같으면 투표 O
// 	// lastLogIndex := len(r.Log) - 1
// 	// lastLogTerm := r.Log[lastLogIndex].Term
// 	// isLogUpToDate := msg.LastLogTerm > lastLogTerm || (msg.LastLogTerm == lastLogTerm && msg.LastLogIndex >= lastLogIndex)

// 	// if (r.VotedFor == "" || r.VotedFor == msg.CandidateID) && isLogUpToDate {
// 	// 	r.VotedFor = msg.CandidateID // 투표를 부여
// 	// 	r.CurrentTerm = msg.Term     // 현재 텀을 업데이트
// 	// 	return true
// 	// }

// 	return false
// }

// func (r *Raft) ProcessResponseAppendEntries(msg *message.ResponseAppendEntries) {

// }

// func (r *Raft) ProcessResponseVote(msg *message.ResponseVote) {

// }

//All Servers:
//commitIndex > lastApplied: lastApplied++, log[lastApplied]를 state machine에 적용
//Request, Response의 term이 currentTerm보다 크면 currentTerm = term, 현재 상태 = follower
