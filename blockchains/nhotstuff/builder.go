package nhotstuff

import (
	"diablo-benchmark/core"
)

type BlockchainBuilder struct {
	logger core.Logger
}

func newBuilder(logger core.Logger) *BlockchainBuilder {
	return &BlockchainBuilder{
		logger: logger,
	}
}

func (b *BlockchainBuilder) CreateAccount(stake int) (interface{}, error) {
	return "hotstuff_account", nil
}

func (b *BlockchainBuilder) CreateContract(name string) (interface{}, error) {
	return "hotstuff_contract", nil
}

func (b *BlockchainBuilder) CreateResource(domain string) (core.SampleFactory, bool) {
	return nil, false
}

func (b *BlockchainBuilder) EncodeTransfer(amount int, from, to interface{}, info core.InteractionInfo) ([]byte, error) {
	return []byte("hotstuff_tx"), nil
}

func (b *BlockchainBuilder) EncodeInvoke(from interface{}, contract interface{}, function string, info core.InteractionInfo) ([]byte, error) {
	return []byte("hotstuff_invoke"), nil
}

func (b *BlockchainBuilder) EncodeInteraction(itype string, expr core.BenchmarkExpression, info core.InteractionInfo) ([]byte, error) {
	return []byte("hotstuff_interaction"), nil
}
