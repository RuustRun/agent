package main

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/RuustRun/agent/internal/contract"
)

// logRing is an io.Writer that captures the agent's own JSON log lines into a
// bounded in-memory ring. The agent tees its slog output here as well as to
// stdout, then ships a snapshot with each status report, so an operator can
// stream a node's agent logs in the console without shell access. Concurrent
// safe: slog may write from any goroutine, and the poll loop snapshots.
type logRing struct {
	mu    sync.Mutex
	lines []contract.LogLine
	max   int
}

func newLogRing(max int) *logRing { return &logRing{max: max} }

// Write is invoked by the slog JSON handler with one complete JSON log line.
func (r *logRing) Write(p []byte) (int, error) {
	ts, text := parseLogLine(p)
	r.mu.Lock()
	r.lines = append(r.lines, contract.LogLine{Ts: ts, Stream: "stdout", Text: text})
	if len(r.lines) > r.max {
		r.lines = r.lines[len(r.lines)-r.max:]
	}
	r.mu.Unlock()
	return len(p), nil
}

// snapshot returns a copy of the current ring, for a status report.
func (r *logRing) snapshot() []contract.LogLine {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]contract.LogLine, len(r.lines))
	copy(out, r.lines)
	return out
}

// parseLogLine turns one JSON slog line into a timestamp and a compact readable
// form ("level msg key=value ..."), falling back to the raw line if it is not the
// JSON we expect. The always-present host attributes are dropped as noise.
func parseLogLine(p []byte) (ts string, text string) {
	ts = time.Now().UTC().Format(time.RFC3339)
	var m map[string]any
	if err := json.Unmarshal(p, &m); err != nil {
		return ts, strings.TrimSpace(string(p))
	}
	if t, ok := m["time"].(string); ok && t != "" {
		ts = t
	}
	delete(m, "time")
	level, _ := m["level"].(string)
	msg, _ := m["msg"].(string)
	delete(m, "level")
	delete(m, "msg")

	var b strings.Builder
	if level != "" {
		b.WriteString(strings.ToLower(level))
		b.WriteByte(' ')
	}
	b.WriteString(msg)
	for k, v := range m {
		if k == "hostId" || k == "regionId" || k == "agentVersion" {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(strings.ReplaceAll(fmtVal(v), "\n", " "))
	}
	return ts, b.String()
}

func fmtVal(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	bb, _ := json.Marshal(v)
	return string(bb)
}
