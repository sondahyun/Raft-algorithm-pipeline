package raft

import (
	"github.com/gitferry/bamboo/blockchain"
	"github.com/gitferry/bamboo/message"
	"github.com/gitferry/bamboo/pacemaker"
	"github.com/gitferry/bamboo/types"
)

type RaftSafety interface {
	// StartElection()
	// ProcessResponseAppendEntries(msg *message.ResponseAppendEntries)    //message.go에서 정의한 ResponseAppendEntries타입 사용
	// ProcessRequestVote(msg *message.RequestVote) bool                   //message.go에서 정의한 RequestVote타입 사용
	// ProcessResponseVote(msg *message.ResponseVote)                      //message.go에서 정의한 ResponseVote타입 사용

	ProcessBlock(block *blockchain.Block) error
	ProcessVote(vote *blockchain.Vote)
	ProcessRemoteTmo(tmo *pacemaker.TMO)
	ProcessElectionLocalTmo(view types.View)
	MakeProposal(view types.View, payload []*message.Transaction) *blockchain.Block
	GetChainStatus() string
}
