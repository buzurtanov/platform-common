package pg

import (
	"context"
	"github.com/buzurtanov/platform-common/pkg/db"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

type pgClient struct {
	masterDBConnect db.DB
}

// New клиент pg
func New(ctx context.Context, dsn string) (db.Client, error) {
	dbc, err := pgxpool.Connect(ctx, dsn)
	if err != nil {
		return nil, errors.Errorf("failed to connect to db: %v", err)
	}

	return &pgClient{
		masterDBConnect: NewDB(dbc),
	}, nil
}

func (c *pgClient) DB() db.DB {
	return c.masterDBConnect
}

func (c *pgClient) Close() error {
	if c.masterDBConnect != nil {
		c.masterDBConnect.Close()
	}

	return nil
}
