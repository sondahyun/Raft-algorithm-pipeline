package mempool

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/gitferry/bamboo/message"
)

type Backend struct {
	txns          *list.List
	limit         int
	totalReceived int64
	*BloomFilter
	mu *sync.Mutex
}

func NewBackend(limit int) *Backend {
	var mu sync.Mutex
	return &Backend{
		txns:        list.New(),
		BloomFilter: NewBloomFilter(),
		mu:          &mu,
		limit:       limit,
	}
}

func (b *Backend) insertBack(txn *message.Transaction) {
	if txn == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.size() > b.limit {
		return
	}
	b.totalReceived++
	b.txns.PushBack(txn)
}

func (b *Backend) insertFront(txn *message.Transaction) {
	if txn == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.txns.PushFront(txn)
}

func (b *Backend) size() int {
	return b.txns.Len()
}

func (b *Backend) front() *message.Transaction {
	if b.size() == 0 {
		return nil
	}
	ele := b.txns.Front()
	val, ok := ele.Value.(*message.Transaction)
	if !ok {
		return nil
	}
	b.txns.Remove(ele)

	return val
}

func (b *Backend) some(n int) []*message.Transaction {
	var batchSize int

	b.mu.Lock() // b.size()와 b.front() 호출 중에 Backend의 상태가 변경되지 않도록 하기 위해 뮤텍스를 사용하여 동기화
	defer b.mu.Unlock()
	batchSize = b.size() // 현재 Backend에 있는 트랜잭션의 수를 batchSize에 저장

	fmt.Printf("total Txn: [%v], n: [%v]\n", batchSize, n)

	if batchSize >= n { // 요청된 트랜잭션 수(n)보다 batchSize가 크거나 같으면, batchSize를 n으로 설정
		batchSize = n
	} else if batchSize < n {
		fmt.Printf("txn 부족\n\n")
	}

	batch := make([]*message.Transaction, 0, batchSize) // batchSize 크기의 슬라이스를 초기화
	for i := 0; i < batchSize; i++ {                    // batchSize 만큼 반복하며, Backend의 트랜잭션을 슬라이스에 추가
		tx := b.front()           // b.front() 메서드를 호출하여 Backend의 맨 앞에 있는 트랜잭션을 가져옴
		batch = append(batch, tx) // 가져온 트랜잭션을 batch 슬라이스에 추가
	}

	return batch // 완성된 트랜잭션 슬라이스를 반환
}

func (b *Backend) addToBloom(id string) {
	b.Add(id)
}
