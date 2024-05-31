package election

import (
	"github.com/gitferry/bamboo/identity"
	"github.com/gitferry/bamboo/types"
)

type raftElection struct {
	leaderManager map[types.View]identity.NodeID
}

func NewRaftElection() *raftElection {
	rl := new(raftElection)
	rl.leaderManager = make(map[types.View]identity.NodeID)

	return rl
}

// func (rl *raftElection) StartElection() {
// 	electionTime := time.Duration(rand.Intn(10)+10) * time.Millisecond
// 	timer := time.NewTimer(electionTime)
// 	select {
// 	case <-timer.C: // Timer 만료
		

// 		//SetState(types.CANDIDATE)
// 		// node.state = types.CANDIDATE

// 		// 새 선거를 시작하는 로직
// 		// 예: r.requestVotes(node)
// 		//AppendEntries RPC를 받으면, timer를 다시 설정
// 		//appendEntriesReceived:
// 		//timer.Reset(electionTimeout)
// 	}
// }

func (rl *raftElection) IsLeader(id identity.NodeID, view types.View) bool {
	return rl.leaderManager[view] == id
}

func (rl *raftElection) FindLeaderFor(view types.View) identity.NodeID {
	return rl.leaderManager[view]
}
