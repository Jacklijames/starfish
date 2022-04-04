package model

import (
	"github.com/transaction-mesh/starfish/pkg/base/meta"
)

// SessionCondition for query GlobalSession
type SessionCondition struct {
	TransactionID      int64
	XID                string
	Status             meta.GlobalStatus
	Statuses           []meta.GlobalStatus
	OverTimeAliveMills int64
}
