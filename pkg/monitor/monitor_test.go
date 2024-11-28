package monitor_test

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jeff-99/mssqlcopy/pkg/monitor"
	"github.com/jeff-99/mssqlcopy/pkg/mssql"
	"github.com/stretchr/testify/assert"
)

func captureOutput(f func() error) (string, error) {
	orig := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	err := f()
	os.Stdout = orig
	w.Close()
	out, _ := io.ReadAll(r)
	return string(out), err
}

func TestMonitorSingleStartEvent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	eventChan := make(chan monitor.Event)
	mon := monitor.NewMonitor(eventChan, false, w)

	go mon.Run(ctx)

	eventChan <- monitor.CopyTaskStartedEvent{
		Table: mssql.TableRef{
			Schema: "dbo",
			Table:  "test",
		},
	}
	
	cancel()
	
	time.Sleep(10 * time.Millisecond)
	w.Close()

	out, _ := io.ReadAll(r)

	assert.Equal(t, "Copying from [dbo].[test]\n\n\r[dbo].[test]   0% |                                                                                                    | (0/10000, 0 it/hr) [0s:0s]", strings.TrimSpace(string(out)),)
}

func TestMonitorMultipleStartEvent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	eventChan := make(chan monitor.Event)
	mon := monitor.NewMonitor(eventChan, false, w)

	go mon.Run(ctx)

	eventChan <- monitor.CopyTaskStartedEvent{
		Table: mssql.TableRef{
			Schema: "dbo",
			Table:  "test",
		},
	}

	eventChan <- monitor.CopyTaskStartedEvent{
		Table: mssql.TableRef{
			Schema: "dbo",
			Table:  "test2",
		},
	}
	
	cancel()
	
	time.Sleep(10 * time.Millisecond)
	w.Close()

	out, _ := io.ReadAll(r)

	assert.Equal(t, 
		"Copying from [dbo].[test2], [dbo].[test]\n\n\r[dbo].[test2]   0% |                                                                                                    | (0/10000, 0 it/hr) [0s:0s]\n\n\r[dbo].[test]   0% |                                                                                                    | (0/10000, 0 it/hr) [0s:0s]",
		 strings.TrimSpace(string(out)))
}
