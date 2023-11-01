package transaction

import (
	"context"
	"github.com/buzurtanov/platform-common/pkg/db"
	"github.com/buzurtanov/platform-common/pkg/db/pg"
	"github.com/jackc/pgx/v4"

	"github.com/pkg/errors"
)

type manager struct {
	db db.Transactor
}

// NewTransactionManager новый менеджер транзакций
func NewTransactionManager(db db.Transactor) db.TransactionManager {
	return &manager{
		db: db,
	}
}

func (m *manager) ReadCommitted(ctx context.Context, handler db.Handler) error {
	txOpts := pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
	return m.transaction(ctx, txOpts, handler)
}

func (m *manager) transaction(ctx context.Context, opts pgx.TxOptions, handler db.Handler) (err error) {
	// Если это вложенная транзакция, пропускаем инициацию новой транзакции и выполняем обработчик.
	tx, ok := ctx.Value(pg.TxKey).(pgx.Tx)
	if ok {
		return handler(ctx)
	}

	// Стартуем новую транзакцию.
	tx, err = m.db.BeginTx(ctx, opts)

	// Кладем транзакцию в контекст.
	pg.MakeContextTx(ctx, tx)

	// Настраиваем функцию отсрочки для отката или коммита транзакции.
	defer func() {
		// восстанавливаемся после паники
		if r := recover(); r != nil {
			err = errors.Errorf("panic recovered: %v", r)
		}

		// откатываем транзакцию, если произошла ошибка
		if err != nil {
			if errRollback := tx.Rollback(ctx); errRollback != nil {
				err = errors.Wrapf(err, "errRollback: %v", errRollback)
			}

			return
		}
		// если ошибок не было, коммитим транзакцию
		err = tx.Commit(ctx)
		if err != nil {
			err = errors.Wrap(err, "tx commit failed")
		}
	}()

	// Выполните код внутри транзакции.
	// Если функция терпит неудачу, возвращаем ошибку, и функция отсрочки выполняет откат
	// или в противном случае транзакция коммитится.
	if err = handler(ctx); err != nil {
		err = errors.Wrap(err, "failed executing code inside transaction")
	}

	return err
}
