package types

type View int

type NodeState byte

const (
	FOLLOWER NodeState = 0 + iota
	CANDIDATE
	LEADER
)
