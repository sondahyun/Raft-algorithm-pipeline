package raft

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gitferry/bamboo/blockchain"
	"github.com/gitferry/bamboo/config"
	"github.com/gitferry/bamboo/crypto"
	"github.com/gitferry/bamboo/election"
	"github.com/gitferry/bamboo/log"
	"github.com/gitferry/bamboo/message"
	"github.com/gitferry/bamboo/node"
	"github.com/gitferry/bamboo/pacemaker"
	"github.com/gitferry/bamboo/types"
)

const FORK = "fork"

type Raft struct {
	node.Node
	election.Election
	pm              *pacemaker.Pacemaker
	lastVotedView   types.View
	preferredView   types.View
	highQC          *blockchain.QC
	bc              *blockchain.BlockChain
	committedBlocks chan *blockchain.Block
	forkedBlocks    chan *blockchain.Block
	bufferedQCs     map[crypto.Identifier]*blockchain.QC
	bufferedBlocks  map[types.View]*blockchain.Block
	mu              sync.Mutex
}

// ProcessElectionLocalTmo implements RaftSafety.
func (r *Raft) ProcessElectionLocalTmo(_ types.View) {
	panic("unimplemented")
}

func NewRaft(node node.Node, pm *pacemaker.Pacemaker, elec election.Election, committedBlocks chan *blockchain.Block, forkedBlocks chan *blockchain.Block) *Raft {
	r := new(Raft)
	r.Node = node
	r.Election = elec
	r.pm = pm
	r.bc = blockchain.NewBlockchain(config.GetConfig().N())
	r.bufferedBlocks = make(map[types.View]*blockchain.Block)
	r.bufferedQCs = make(map[crypto.Identifier]*blockchain.QC)
	r.highQC = &blockchain.QC{View: 0}
	r.committedBlocks = committedBlocks
	r.forkedBlocks = forkedBlocks

	return r
}

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

	if block.QC == nil {
		return errors.New("the block should contain a QC")
	}
	r.updateHighQC(block.QC)

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
		log.Errorf("[%v] cannot decide whether to vote the block: %v", r.ID(), err)

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

func (r *Raft) ProcessVote(vote *blockchain.Vote) {
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

func (r *Raft) ProcessRemoteTmo(tmo *pacemaker.TMO) {
	log.Debugf("[%v] is processing tmo from %v", r.ID(), tmo.NodeID)
	r.processCertificate(tmo.HighQC)
	isBuilt, tc := r.pm.ProcessRemoteTmo(tmo)
	if !isBuilt {
		return
	}
	log.Debugf("[%v] a tc is built for view %v", r.ID(), tc.View)
	r.processTC(tc)
}

func (r *Raft) ProcessLocalTmo(view types.View) {
	r.pm.AdvanceView(view)
	tmo := &pacemaker.TMO{
		View:   view + 1,
		NodeID: r.ID(),
		HighQC: r.GetHighQC(),
	}
	r.Broadcast(tmo)
	r.ProcessRemoteTmo(tmo)
}

func (r *Raft) MakeProposal(view types.View, payload []*message.Transaction) *blockchain.Block {
	qc := r.forkChoice()
	block := blockchain.MakeBlock(view, qc, qc.BlockID, payload, r.ID())

	return block
}

func (r *Raft) forkChoice() *blockchain.QC {
	var choice *blockchain.QC
	if !r.IsByz() || config.GetConfig().Strategy != FORK {
		return r.GetHighQC()
	}
	//	create a fork by returning highQC's parent's QC
	parBlockID := r.GetHighQC().BlockID
	parBlock, err := r.bc.GetBlockByID(parBlockID)
	if err != nil {
		log.Warningf("cannot get parent block of block id: %x: %v", parBlockID, err)
	}
	if parBlock.QC.View < r.preferredView {
		choice = r.GetHighQC()
	} else {
		choice = parBlock.QC
	}
	// to simulate TC's view
	choice.View = r.pm.GetCurView() - 1

	return choice
}

func (r *Raft) processTC(tc *pacemaker.TC) {
	if tc.View < r.pm.GetCurView() {
		return
	}
	r.pm.AdvanceView(tc.View)
}

func (r *Raft) GetHighQC() *blockchain.QC {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.highQC
}

func (r *Raft) GetChainStatus() string {
	chainGrowthRate := r.bc.GetChainGrowth()
	blockIntervals := r.bc.GetBlockIntervals()

	return fmt.Sprintf("[%v] The current view is: %v, chain growth rate is: %v, ave block interval is: %v", r.ID(), r.pm.GetCurView(), chainGrowthRate, blockIntervals)
}

func (r *Raft) updateHighQC(qc *blockchain.QC) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if qc.View > r.highQC.View {
		r.highQC = qc
	}
}

func (r *Raft) processCertificate(qc *blockchain.QC) {
	log.Debugf("[%v] is processing a QC, block id: %x", r.ID(), qc.BlockID)
	if qc.View < r.pm.GetCurView() {
		return
	}
	if qc.Leader != r.ID() {
		quorumIsVerified, _ := crypto.VerifyQuorumSignature(qc.AggSig, qc.BlockID, qc.Signers)
		if !quorumIsVerified {
			log.Warningf("[%v] received a quorum with invalid signatures", r.ID())

			return
		}
	}
	if r.IsByz() && config.GetConfig().Strategy == FORK && r.IsLeader(r.ID(), qc.View+1) {
		r.pm.AdvanceView(qc.View)

		return
	}
	err := r.updatePreferredView(qc)
	if err != nil {
		r.bufferedQCs[qc.BlockID] = qc
		log.Debugf("[%v] a qc is buffered, view: %v, id: %x", r.ID(), qc.View, qc.BlockID)

		return
	}
	r.pm.AdvanceView(qc.View)
	r.updateHighQC(qc)
	if qc.View < 3 {
		return
	}
	ok, block, _ := r.commitRule(qc)
	if !ok {
		return
	}
	// forked blocks are found when pruning
	committedBlocks, forkedBlocks, err := r.bc.CommitBlock(block.ID, r.pm.GetCurView())
	if err != nil {
		log.Errorf("[%v] cannot commit blocks, %v", r.ID(), err)

		return
	}
	for _, cBlock := range committedBlocks {
		r.committedBlocks <- cBlock
	}
	for _, fBlock := range forkedBlocks {
		r.forkedBlocks <- fBlock
	}
}

func (r *Raft) votingRule(block *blockchain.Block) (bool, error) {
	if block.View <= 2 {
		return true, nil
	}
	parentBlock, err := r.bc.GetParentBlock(block.ID)
	if err != nil {
		return false, fmt.Errorf("cannot vote for block: %w", err)
	}
	if (block.View <= r.lastVotedView) || (parentBlock.View < r.preferredView) {
		return false, nil
	}

	return true, nil
}

func (r *Raft) commitRule(qc *blockchain.QC) (bool, *blockchain.Block, error) {
	parentBlock, err := r.bc.GetParentBlock(qc.BlockID)
	if err != nil {
		return false, nil, fmt.Errorf("cannot commit any block: %w", err)
	}
	grandParentBlock, err := r.bc.GetParentBlock(parentBlock.ID)
	if err != nil {
		return false, nil, fmt.Errorf("cannot commit any block: %w", err)
	}
	if ((grandParentBlock.View + 1) == parentBlock.View) && ((parentBlock.View + 1) == qc.View) {
		return true, grandParentBlock, nil
	}

	return false, nil, nil
}

// func (r *Raft) updateLastVotedView(targetView types.View) error {
// 	if targetView < r.lastVotedView {
// 		return errors.New("target view is lower than the last voted view")
// 	}
// 	r.lastVotedView = targetView
// 	return nil
// }

func (r *Raft) updatePreferredView(qc *blockchain.QC) error {
	if qc.View <= 2 {
		return nil
	}
	_, err := r.bc.GetBlockByID(qc.BlockID)
	if err != nil {
		return fmt.Errorf("cannot update preferred view: %w", err)
	}
	grandParentBlock, err := r.bc.GetParentBlock(qc.BlockID)
	if err != nil {
		return fmt.Errorf("cannot update preferred view: %w", err)
	}
	if grandParentBlock.View > r.preferredView {
		r.preferredView = grandParentBlock.View
	}

	return nil
}
