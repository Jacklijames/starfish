package producer_test

import (
	"database/sql"
	"os"
	"testing"
)

import (
	"github.com/transaction-mesh/starfish/pkg/test"
	"github.com/transaction-mesh/starfish/pkg/util/log"
)

var (
	utContainerMySQL *sql.DB
)

func TestMain(m *testing.M) {
	var err error
	testContainer := &test.MysqlContainer{
		Username: "root",
		Password: "123456",
		Database: "starfish",
	}
	ctx, container := test.SetupMysql(testContainer)
	utContainerMySQL, err = testContainer.OpenConnection(ctx, container)
	defer test.CloseConnection(ctx, container)
	if err != nil {
		log.Errorf("failed to setup MySQL container")
		panic(err)
	}
	os.Exit(m.Run())
}

func TestProducerSelect(t *testing.T) {
	query, err := utContainerMySQL.Query("select * from branch_table")
	if err != nil {
		t.Errorf("error on list branch_table : %s", err)
	}
	defer query.Close()
}
