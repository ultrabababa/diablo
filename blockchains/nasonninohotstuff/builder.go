package nasonninohotstuff

import (
	"diablo-benchmark/core"
	"encoding/binary"
	"sync/atomic"
)

type BlockchainBuilder struct {
	logger core.Logger
	seq    uint64
}

func newBuilder(logger core.Logger) *BlockchainBuilder {
	return &BlockchainBuilder{logger: logger}
}

func (b *BlockchainBuilder) CreateAccount(stake int) (interface{}, error) {
	return "asonnino_hotstuff_account", nil
}

func (b *BlockchainBuilder) CreateContract(name string) (interface{}, error) {
	return "asonnino_hotstuff_contract", nil
}

func (b *BlockchainBuilder) CreateResource(domain string) (core.SampleFactory, bool) {
	return nil, false
}

func (b *BlockchainBuilder) EncodeTransfer(amount int, from, to interface{}, info core.InteractionInfo) ([]byte, error) {
	return b.encodePayload(), nil
}

func (b *BlockchainBuilder) EncodeInvoke(from interface{}, contract interface{}, function string, info core.InteractionInfo) ([]byte, error) {
	return b.encodePayload(), nil
}

func (b *BlockchainBuilder) EncodeInteraction(itype string, expr core.BenchmarkExpression, info core.InteractionInfo) ([]byte, error) {
	return b.encodePayload(), nil
}

func (b *BlockchainBuilder) encodePayload() []byte {
	seq := atomic.AddUint64(&b.seq, 1)
	payload := make([]byte, 9)
	payload[0] = 0 // sample tx marker for asonnino mempool logs
	binary.BigEndian.PutUint64(payload[1:9], seq)
	return payload
}
