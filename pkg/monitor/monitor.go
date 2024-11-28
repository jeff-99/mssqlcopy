package monitor

import (
	"context"
	"os"

	// "encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/jeff-99/mssqlcopy/pkg/mssql"
	"github.com/schollz/progressbar/v3"
)

type Event interface{}

type ProgressUpdateEvent struct {
	RowsCopied int            `json:"rows_copied"`
	Table      mssql.TableRef `json:"table"`
}

type CopyTaskStartedEvent struct {
	Table mssql.TableRef `json:"table"`
}

type CopyTaskFinishedEvent struct {
	Table mssql.TableRef `json:"table"`
}

type CountUpdateEvent struct {
	TotalRows int            `json:"total_rows"`
	Table     mssql.TableRef `json:"table"`
}

type ErrorEvent struct {
	Table mssql.TableRef `json:"table"`
	Err   error          `json:"error"`
}

type LastRender struct {
	managedLines int
	rowsCopied   map[string]int
}

type Monitor struct {
	eventChan    <-chan Event
	ci           bool
	monitors     map[string]*ProgressReporter
	renderTicker *time.Ticker

	lastRender      *LastRender
	sortedTableKeys []string

	w io.Writer
}

func NewMonitor(eventChan <-chan Event, ci bool, w io.Writer) *Monitor {
	if w == nil {
		w = os.Stdout
	}

	return &Monitor{
		eventChan:    eventChan,
		monitors:     make(map[string]*ProgressReporter),
		renderTicker: time.NewTicker(10 * time.Millisecond),
		ci:           ci,
		lastRender: &LastRender{
			managedLines: 0,
			rowsCopied:   make(map[string]int),
		},
		w: w,
	}
}

func (m *Monitor) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			m.render()
			return nil
		case event := <-m.eventChan:
			// m.logEvent(event)

			switch e := event.(type) {
			case ProgressUpdateEvent:
				if _, ok := m.monitors[e.Table.String()]; !ok {
					return fmt.Errorf("no monitor found for table %s", e.Table.String())
				}
				m.monitors[e.Table.String()].Update(e.RowsCopied)
			case CopyTaskStartedEvent:
				if _, ok := m.monitors[e.Table.String()]; !ok {
					m.monitors[e.Table.String()] = NewProgressReporter(e.Table)
				} else {
					return fmt.Errorf("monitor already exists for table %s", e.Table.String())
				}

				sortedKeys := make([]string, len(m.monitors))
				i := 0
				for k := range m.monitors {
					sortedKeys[i] = k
					i++
				}
				sort.Strings(sortedKeys)
				m.sortedTableKeys = sortedKeys

			case CountUpdateEvent:
				if _, ok := m.monitors[e.Table.String()]; !ok {
					return fmt.Errorf("no monitor found for table %s", e.Table.String())
				}
				m.monitors[e.Table.String()].SetTotalRows(e.TotalRows)
			case CopyTaskFinishedEvent:
				if _, ok := m.monitors[e.Table.String()]; !ok {
					return fmt.Errorf("no monitor found for table %s", e.Table.String())
				}

				m.monitors[e.Table.String()].done = true
				anyRunning := false
				for _, monitor := range m.monitors {
					if !monitor.done {
						anyRunning = true
						break
					}
				}

				if !anyRunning {
					m.renderTicker.Stop()
					m.render()
					return nil
				}
			case ErrorEvent:
				if _, ok := m.monitors[e.Table.String()]; !ok {
					return fmt.Errorf("no monitor found for table %s", e.Table.String())
				}
				m.monitors[e.Table.String()].SetError(e.Err)

				anyRunning := false
				for _, monitor := range m.monitors {
					if !monitor.done {
						anyRunning = true
						break
					}
				}

				if !anyRunning {
					m.renderTicker.Stop()
					m.render()
					return nil
				}
			}

		case <-m.renderTicker.C:
			m.render()
		}
	}

}

func (m *Monitor) render() {
	if m.ci {
		for _, key := range m.sortedTableKeys {
			if m.monitors[key].RowTotal == 0 {
				continue
			}
			if _, ok := m.lastRender.rowsCopied[key]; !ok {
				m.w.Write([]byte(fmt.Sprintf("%s copied %d of %d\n", key, m.monitors[key].RowsCopied, m.monitors[key].RowTotal)))
				m.lastRender.rowsCopied[key] = m.monitors[key].RowsCopied
				continue
			}

			lastCount := m.lastRender.rowsCopied[key]
			currentCount := m.monitors[key].RowsCopied

			if lastCount < currentCount {
				m.w.Write([]byte(fmt.Sprintf("%s copied %d of %d\n", key, currentCount, m.monitors[key].RowTotal)))
				m.lastRender.rowsCopied[key] = m.monitors[key].RowsCopied
			}
		}

		return

	}
	if m.lastRender.managedLines > 0 {
		// clear terminal output
		for i := 0; i < m.lastRender.managedLines; i++ {
			m.w.Write([]byte(fmt.Sprint("\033[1F\033[2K"))) // Move cursor up and clear line
		}
	}

	newManagedLines := 0

	var output strings.Builder

	output.WriteString(fmt.Sprintf("Copying from %s\n\n", strings.Join(m.sortedTableKeys, ", ")))
	newManagedLines++
	newManagedLines++

	for _, key := range m.sortedTableKeys {
		bar := m.monitors[key]
		barString := bar.bar.String()

		if bar.err == nil {
			output.WriteString(fmt.Sprintf("%s\n\n", barString))
			newManagedLines++
			newManagedLines++
		} else {
			output.WriteString(fmt.Sprintf("%s = FAILED : %s\n\n", bar.Table.String(), bar.err))
			newManagedLines++
			newManagedLines++
		}

	}

	m.w.Write([]byte(output.String()))

	m.lastRender.managedLines = newManagedLines

}

type ProgressReporter struct {
	bar        *progressbar.ProgressBar
	RowTotal   int
	RowsCopied int
	Table      mssql.TableRef
	done       bool
	err        error
}

func NewProgressReporter(table mssql.TableRef) *ProgressReporter {
	return &ProgressReporter{
		bar: progressbar.NewOptions(10_000,
			progressbar.OptionSetDescription(table.String()),
			progressbar.OptionSetWriter(io.Discard),
			progressbar.OptionSetWidth(100),
			progressbar.OptionThrottle(65*time.Millisecond),
			progressbar.OptionShowCount(),
			progressbar.OptionShowIts(),
			progressbar.OptionSpinnerType(14),
			progressbar.OptionSetRenderBlankState(true),
			progressbar.OptionSetPredictTime(true),
			progressbar.OptionSetRenderBlankState(true),
		),
		RowTotal: 0,
		Table:    table,
		done:     false,
	}
}

func (p *ProgressReporter) Update(rowsCopied int) {
	p.RowsCopied = p.RowsCopied + rowsCopied
	p.bar.Add(rowsCopied)
}

func (p *ProgressReporter) SetTotalRows(totalRows int) {
	p.RowTotal = totalRows
	p.bar.ChangeMax(totalRows)
}

func (p *ProgressReporter) SetError(err error) {
	p.done = true
	p.err = err
}
