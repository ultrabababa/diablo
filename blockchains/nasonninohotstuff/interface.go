package nasonninohotstuff

import "diablo-benchmark/core"

type BlockchainInterface struct{}

func (i *BlockchainInterface) Builder(params map[string]string, env []string, endpoints map[string][]string, logger core.Logger) (core.BlockchainBuilder, error) {
	return newBuilder(logger), nil
}

func (i *BlockchainInterface) Client(params map[string]string, env, view []string, logger core.Logger) (core.BlockchainClient, error) {
	cfg, err := parseClientConfig(params)
	if err != nil {
		return nil, err
	}
	return newClient(logger, view, cfg)
}
