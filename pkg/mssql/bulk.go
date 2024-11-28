package mssql

import (
	"context"
	"database/sql"
	"fmt"

	mssqlDriver "github.com/microsoft/go-mssqldb"
)

type BulkInsert struct {
	table       TableRef
	columns     []string
	db          *sql.DB
	commitCount int

	count int
	stmt  *sql.Stmt
	tx    *sql.Tx
}

func NewBulkInsert(table TableRef, columns []string, db *sql.DB) *BulkInsert {
	commitCount := 50_000

	return &BulkInsert{
		table:       table,
		columns:     columns,
		db:          db,
		commitCount: commitCount,
	}
}

func (bi *BulkInsert) getStmt(ctx context.Context) (*sql.Stmt, error) {
	if bi.stmt == nil {
		tx, err := bi.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}

		query := mssqlDriver.CopyIn(fmt.Sprintf("%s.%s", bi.table.Schema, bi.table.Table), mssqlDriver.BulkOptions{}, bi.columns...)
		stmt, err := tx.Prepare(query)
		if err != nil {
			return nil, err
		}

		bi.stmt = stmt
		bi.tx = tx

	}

	return bi.stmt, nil

}

func (bi *BulkInsert) Insert(ctx context.Context, row []interface{}) error {
	// decimals are read as []uint8 by the driver, []uint8 is a byte slice (alias for []byte) but the same driver does not support []byte for bulk insert so we need to convert it to string
	for i, value := range row {
		if v, ok := value.(*interface{}); ok {
			if b, ok := (*v).([]uint8); ok {
				row[i] = string(b)
			}
		}
	}

	stmt, err := bi.getStmt(ctx)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx, row...)
	if err != nil {
		return err
	}

	bi.count++
	if bi.count%bi.commitCount == 0 {
		err = bi.Commit(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bi *BulkInsert) Commit(ctx context.Context) error {
	if bi.stmt == nil {
		return nil
	}

	_, err := bi.stmt.Exec()
	if err != nil {
		return err
	}

	err = bi.stmt.Close()
	if err != nil {
		return err
	}

	err = bi.tx.Commit()
	if err != nil {
		return err
	}

	bi.count = 0
	bi.stmt = nil
	bi.tx = nil

	return nil
}

func (bi *BulkInsert) Rollback(ctx context.Context) error {
	if bi.tx == nil {
		return fmt.Errorf("no active transaction to rollback")
	}
	return bi.tx.Rollback()
}
