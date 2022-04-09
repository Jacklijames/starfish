package test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
)

import (
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

import (
	"github.com/transaction-mesh/starfish/pkg/util/log"
)

type resCon struct {
	context.Context
	testcontainers.Container
}

type mysqlContainer struct {
	username string `validate:"required" yaml:"username" json:"username"`
	database string `validate:"required" yaml:"database" json:"database"`
	password string `validate:"required" yaml:"password" json:"password"`
}

var (
	db *sql.DB
)

func setupMysql(tester *mysqlContainer) resCon {
	log.Info("setup mysql container")
	ctx := context.Background()
	seedDataPath, err := os.Getwd()
	if err != nil {
		log.Errorf("Error get working directory: %s", err)
		panic(fmt.Sprintf("%v", err))
	}
	mountPath := seedDataPath + "/../testcontainer/integration"
	req := testcontainers.ContainerRequest{
		Image: "mysql:lasted",
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": tester.password,
			"MYSQL_DATABASE":      tester.database,
		},
		ExposedPorts: []string{"3306/tcp", "33060/tcp"},
		BindMounts: map[string]string{
			"/docker-entrypoint-initdb.d": mountPath,
		},
		WaitingFor: wait.ForLog("* Ready to accept connections"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Errorf("Error Start MySQL container: %s", err)
		panic(fmt.Sprintf("%v", err))
	}
	return resCon{ctx, container}
}

func (tester mysqlContainer) OpenConnection(resC resCon) (*sql.DB, error) {
	host, err := resC.Container.Host(resC.Context)
	p, err := resC.Container.MappedPort(resC.Context, "3306/tcp")
	port := p.Int()
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=skip-verify&parseTime=true&multiStatements=true",
		tester.username, tester.password, host, port, tester.database)

	db, err = sql.Open("mysql", connectionString)

	if err != nil {
		log.Error("error connect to db: %+v\n", err)
	}

	if err = db.Ping(); err != nil {
		log.Errorf("error pinging db: %+v\n", err)
	}
	return db, err
}

func CloseConnection(resC resCon) {
	log.Info("Closing Container")
	err := resC.Container.Terminate(resC.Context)
	if err != nil {
		log.Errorf("error stop Container: %s", err)
		panic(fmt.Sprintf("%v", err))
	}
}
