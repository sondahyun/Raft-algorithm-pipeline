// view에 따라서 리더가 순환, 변경
// 초기 뷰(3이하)에서는 설정된 peer수보다 작은 노드 ID를 가진 노드가 리더가 됨
// 그 이후에는 뷰번호와 peer수를 사용하여 해시 함수를 통해 리더를 결정 (view가 증가함에따라서 리더가 바뀜)
package election

import (
	"crypto/sha1"
	"encoding/binary"
	"strconv"

	"github.com/gitferry/bamboo/identity"
	"github.com/gitferry/bamboo/types"
)

type Rotation struct {
	peerNo int
}

// StartElection implements Election.
func (r *Rotation) StartElection() {
	panic("unimplemented")
}

func NewRotation(peerNo int) *Rotation {
	return &Rotation{
		peerNo: peerNo,
	}
}

func (r *Rotation) IsLeader(id identity.NodeID, view types.View) bool {
	if view <= 3 {
		if id.Node() < r.peerNo {
			return false
		}
		return true
	}
	h := sha1.New()
	h.Write([]byte(strconv.Itoa(int(view) + 1)))
	bs := h.Sum(nil)
	data := binary.BigEndian.Uint64(bs)
	return data%uint64(r.peerNo) == uint64(id.Node()-1)
}

func (r *Rotation) FindLeaderFor(view types.View) identity.NodeID {
	if view <= 3 {
		return identity.NewNodeID(r.peerNo)
	}
	h := sha1.New()
	h.Write([]byte(strconv.Itoa(int(view + 1))))
	bs := h.Sum(nil)
	data := binary.BigEndian.Uint64(bs)
	id := data%uint64(r.peerNo) + 1
	return identity.NewNodeID(int(id))
}
