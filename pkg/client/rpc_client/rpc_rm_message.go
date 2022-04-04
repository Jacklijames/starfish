package rpc_client

import (
	"github.com/transaction-mesh/starfish/pkg/base/protocal"
)

type RpcRMMessage struct {
	RpcMessage    protocal.RpcMessage
	ServerAddress string
}
