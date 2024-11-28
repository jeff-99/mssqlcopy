package cli

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jeff-99/mssqlcopy/pkg/copy"
	"github.com/jeff-99/mssqlcopy/pkg/monitor"
	"github.com/jeff-99/mssqlcopy/pkg/mssql"
)

func chunkBy[T any](items []T, chunkSize int) (chunks [][]T) {
	var _chunks = make([][]T, 0, (len(items)/chunkSize)+1)
	for chunkSize < len(items) {
		items, _chunks = items[chunkSize:], append(_chunks, items[0:chunkSize:chunkSize])
	}
	return append(_chunks, items)
}

func Copy(sourceHost, sourceDB, targetHost, targetDB, schema, filter, queryFilter string, parrallel int, ci bool) {
	sDB, err := mssql.Connect(sourceHost, sourceDB)
	if err != nil {
		log.Fatal(err)
	}
	defer sDB.Close()

	tDB, err := mssql.Connect(targetHost, targetDB)
	if err != nil {
		log.Fatal(err)
	}
	defer tDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()

	eventChan := make(chan monitor.Event, 1000)
	wg := sync.WaitGroup{}
	wg.Add(1)

	monitor := monitor.NewMonitor(eventChan, ci, nil)
	go func() {
		defer wg.Done()
		monitor.Run(ctx)
	}()

	tables, err := sDB.GetTablesFromFilter(ctx, schema, filter)
	if err != nil {
		log.Fatal(err)
	}

	if len(tables) == 0 {
		log.Fatal("No tables")
	}

	tasks := make([]*copy.CopyTask, len(tables))

	for i, table := range tables {
		task := copy.NewCopyTask(mssql.TableRef{Schema: schema, Table: table}, sDB, tDB, queryFilter, eventChan)
		tasks[i] = task
	}

	for _, chunk := range chunkBy(tasks, parrallel) {
		for _, task := range chunk {
			go task.Run(ctx)
		}

		for _, task := range chunk {
			task.Wait()
		}
	}

	cancel()
	wg.Wait()
}
