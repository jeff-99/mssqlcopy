package copy

import (
	"context"
	"fmt"
	"sync"

	"github.com/jeff-99/mssqlcopy/pkg/monitor"
	"github.com/jeff-99/mssqlcopy/pkg/mssql"
)

type CopyTask struct {
	table mssql.TableRef

	wg       *sync.WaitGroup
	sourceDB *mssql.MSSQLDB
	targetDB *mssql.MSSQLDB

	queryFilter string

	eventChan chan<- monitor.Event

	isRunning bool
	errs      []error
}

func NewCopyTask(table mssql.TableRef, sourceDB *mssql.MSSQLDB, targetDB *mssql.MSSQLDB, queryFilter string, eventChan chan<- monitor.Event) *CopyTask {
	wg := sync.WaitGroup{}
	wg.Add(2)

	return &CopyTask{
		table: table,

		sourceDB: sourceDB,
		targetDB: targetDB,

		queryFilter: queryFilter,

		eventChan: eventChan,

		isRunning: false,
		wg:        &wg,

		errs: make([]error, 0),
	}

}

func (ct *CopyTask) Wait() error {
	ct.wg.Wait()

	if len(ct.errs) > 0 {
		return fmt.Errorf("Errors encountered: %v", ct.errs)
	}

	return nil
}


func (ct *CopyTask) Run(ctx context.Context) error {
	dataChan := make(chan []interface{}, 1000)

	ct.eventChan <- monitor.CopyTaskStartedEvent{Table: ct.table}

	targetSchema, err := ct.targetDB.GetSchemaDefinition(ctx, ct.table)
	if err != nil {
		ct.eventChan <- monitor.ErrorEvent{
			Table: ct.table,
			Err:   fmt.Errorf("Failed to get schema for table %s from the targetDB", ct.table),
		}
		ct.wg.Done()
		ct.wg.Done()
		return err
	}

	targetColumns := make([]string, 0, len(targetSchema))
	for column := range targetSchema {
		targetColumns = append(targetColumns, column)
	}

	go func() {
		defer close(dataChan)
		defer ct.wg.Done()
		sourceSchema, err := ct.sourceDB.GetSchemaDefinition(ctx, ct.table)
		if err != nil {
			_ = append(ct.errs, err)
			ct.eventChan <- monitor.ErrorEvent{
				Table: ct.table,
				Err:   fmt.Errorf("Failed to get schema for table %s from the sourceDB", ct.table),
			}
			return
		}

		if !compareSchemas(sourceSchema, targetSchema) {
			_ = append(ct.errs, err)
			ct.eventChan <- monitor.ErrorEvent{
				Table: ct.table,
				Err:   fmt.Errorf("Schema mismatch detected between Source and Target DBs on table %s", ct.table),
			}

			return
		}

		numberOfRows, err := ct.sourceDB.GetCount(ctx, ct.table, ct.queryFilter)
		if err != nil {
			_ = append(ct.errs, err)
			ct.eventChan <- monitor.ErrorEvent{
				Table: ct.table,
				Err:   fmt.Errorf("Failed to get count for table %s from the sourceDB", ct.table),
			}
			return
		}
		ct.eventChan <- monitor.CountUpdateEvent{TotalRows: numberOfRows, Table: ct.table}

		rows, err := ct.sourceDB.SelectFrom(ctx, ct.table, targetColumns, ct.queryFilter)
		if err != nil {
			_ = append(ct.errs, err)
			ct.eventChan <- monitor.ErrorEvent{
				Table: ct.table,
				Err:   fmt.Errorf("Failed to select data from source table %s, %s", ct.table, err),
			}
			return
		}

		for {
			values, err := rows.Next()
			if err != nil {
				_ = append(ct.errs, err)
				ct.eventChan <- monitor.ErrorEvent{
					Table: ct.table,
					Err:   fmt.Errorf("Failed to get the Next row from the source table %s", ct.table),
				}
			}

			if len(values) == 0 {
				break
			}

			dataChan <- values
		}
	}()

	go func() {
		defer ct.wg.Done()

		bulkInsert, err := ct.targetDB.BulkInsert(ctx, ct.table, targetColumns)

		i := 0
		var fks []mssql.ForeingKeyConstraint
		for row := range dataChan {
			if i == 0 {
				// only drop and recreate foreign keys if we are inserting data
				fks, err = ct.targetDB.GetReferencedForeignKeys(ctx, ct.table)
				if err != nil {
					_ = append(ct.errs, err)
					ct.eventChan <- monitor.ErrorEvent{
						Table: ct.table,
						Err:   fmt.Errorf("Failed to get foreign keys for table %s from the targetDB", ct.table),
					}
					return
				}

				err = ct.targetDB.DropReferencedForeignKeys(ctx, ct.table)
				if err != nil {
					_ = append(ct.errs, err)
					ct.eventChan <- monitor.ErrorEvent{
						Table: ct.table,
						Err:   fmt.Errorf("Failed to drop foreign keys for table %s from the targetDB", ct.table),
					}
					return
				}

				err = ct.targetDB.EmptyTable(ctx, ct.table)
				if err != nil {
					_ = append(ct.errs, err)
					ct.eventChan <- monitor.ErrorEvent{
						Table: ct.table,
						Err:   fmt.Errorf("Failed to empty target table %s", ct.table),
					}

					return
				}
			}

			i++

			err := bulkInsert.Insert(ctx, row)
			if err != nil {
				bulkInsert.Rollback(ctx)
				_ = append(ct.errs, err)
				ct.eventChan <- monitor.ErrorEvent{
					Table: ct.table,
					Err:   fmt.Errorf("Failed to insert row into the target table %s, %s", ct.table, err),
				}
				return
			}
			ct.eventChan <- monitor.ProgressUpdateEvent{RowsCopied: 1, Table: ct.table}

		}

		err = bulkInsert.Commit(ctx)
		if err != nil {
			_ = append(ct.errs, err)
			ct.eventChan <- monitor.ErrorEvent{
				Table: ct.table,
				Err:   fmt.Errorf("Failed to commit the transaction into target table %s, %s", ct.table, err),
			}
			return
		}

		if len(fks) > 0 {

			err = ct.targetDB.AddForeignKeys(ctx, fks)
			if err != nil {
				_ = append(ct.errs, err)
				ct.eventChan <- monitor.ErrorEvent{
					Table: ct.table,
					Err:   fmt.Errorf("Failed to add foreign keys into target table %s, %s", ct.table, err),
				}
				return
			}
		}

		ct.eventChan <- monitor.CopyTaskFinishedEvent{Table: ct.table}

	}()

	return nil
}

func compareSchemas(sourceSchema, targetSchema map[string]string) bool {
	if len(sourceSchema) != len(targetSchema) {
		return false
	}

	for column, dataType := range sourceSchema {
		if targetSchema[column] != dataType {
			return false
		}
	}

	return true
}