package raft

import (
	"github.com/gitferry/bamboo/message"
)

type RaftSafety interface {
	ProcessRequestAppendEntries(msg *message.RequestAppendEntries)   //message.go에서 정의한 RequestAppendEntries타입 사용
	ProcessResponseAppendEntries(msg *message.ResponseAppendEntries) //message.go에서 정의한 ResponseAppendEntries타입 사용
	ProcessRequestVote(msg *message.RequestVote)                     //message.go에서 정의한 RequestVote타입 사용
	ProcessResponseVote(msg *message.ResponseVote)                   //message.go에서 정의한 ResponseVote타입 사용
}
