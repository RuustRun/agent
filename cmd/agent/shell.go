package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/RuustRun/agent/internal/contract"
	"github.com/RuustRun/agent/internal/docker"
)

// shellMaxDuration bounds a single interactive shell session, so a forgotten terminal
// cannot hold an exec open on a container forever.
const shellMaxDuration = 60 * time.Minute

// handleShellSessions bridges a PTY for each newly-seen pending shell session whose
// workload runs on this host. A session stays in desired-state until it is closed, so we
// track the ones we are already handling and skip them. Never blocks the reconcile loop.
func (a *agent) handleShellSessions(ctx context.Context, reqs []contract.ShellSessionRequest) {
	for _, req := range reqs {
		if req.ID == "" || req.WorkloadID == "" {
			continue
		}
		if _, loaded := a.shellActive.LoadOrStore(req.ID, true); loaded {
			continue // already handling this session
		}
		go func(req contract.ShellSessionRequest) {
			defer a.shellActive.Delete(req.ID)
			if err := a.runShellSession(ctx, req); err != nil {
				a.log.Warn("shell session ended", "session", req.ID, "err", err)
			}
		}(req)
	}
}

// runShellSession finds the workload's live container, dials the relay, and bridges the
// WebSocket to an interactive PTY in that container. Blocks until the session ends.
func (a *agent) runShellSession(ctx context.Context, req contract.ShellSessionRequest) error {
	// Resolve the live container for this workload. We only ever exec into a container
	// this host actually runs: ownership defence at the agent, on top of the CP's check.
	containers, err := a.docker.List(ctx)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}
	var containerID string
	for _, c := range containers {
		if c.WorkloadID == req.WorkloadID && c.State == contract.StateRunning {
			containerID = c.ID
			break
		}
	}
	if containerID == "" {
		return fmt.Errorf("no running container for workload %q", req.WorkloadID)
	}

	// Dial the relay's agent endpoint, authenticated as this host.
	wsURL := toWebSocketURL(strings.TrimRight(a.cfg.controlPlaneURL, "/")) +
		"/api/" + contract.APIVersion + "/shell/relay/agent?session=" + url.QueryEscape(req.ID)
	header := http.Header{}
	header.Set("Authorization", "Bearer "+a.cfg.hostToken)
	header.Set("User-Agent", "ruust-agent/"+agentVersion)

	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, _, err := websocket.DefaultDialer.DialContext(dialCtx, wsURL, header)
	if err != nil {
		return fmt.Errorf("dial relay: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Bound the whole session so a shell never runs forever.
	sessCtx, sessCancel := context.WithTimeout(ctx, shellMaxDuration)
	defer sessCancel()

	// Bridge: WS binary frames -> shell stdin; WS text frames (JSON resize) -> resize;
	// shell terminal output -> WS binary frames.
	stdinR, stdinW := io.Pipe()
	resize := make(chan docker.ShellResize, 8)
	out := &wsBinaryWriter{conn: conn}

	go func() {
		defer func() { _ = stdinW.Close() }()
		defer sessCancel()
		for {
			mt, data, rerr := conn.ReadMessage()
			if rerr != nil {
				return
			}
			switch mt {
			case websocket.BinaryMessage:
				if _, werr := stdinW.Write(data); werr != nil {
					return
				}
			case websocket.TextMessage:
				var m struct {
					Type string `json:"type"`
					Rows uint   `json:"rows"`
					Cols uint   `json:"cols"`
				}
				if json.Unmarshal(data, &m) == nil && m.Type == "resize" {
					select {
					case resize <- docker.ShellResize{Rows: m.Rows, Cols: m.Cols}:
					default:
					}
				}
			}
		}
	}()

	a.log.Info("shell session started", "session", req.ID, "workloadId", req.WorkloadID)
	shErr := a.docker.Shell(sessCtx, containerID, stdinR, out, resize)
	// Best-effort notify the client the shell exited; safe here because the shell's
	// output copier has finished, so no other goroutine is writing to the connection.
	_ = out.writeText([]byte(`{"type":"exit"}`))
	return shErr
}

// wsBinaryWriter turns Write calls into binary WebSocket frames. gorilla connections are
// not safe for concurrent writers, so all writes are serialised through the mutex.
type wsBinaryWriter struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (w *wsBinaryWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *wsBinaryWriter) writeText(p []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.WriteMessage(websocket.TextMessage, p)
}

// toWebSocketURL swaps an http(s) base for its ws(s) equivalent.
func toWebSocketURL(base string) string {
	if strings.HasPrefix(base, "https://") {
		return "wss://" + strings.TrimPrefix(base, "https://")
	}
	if strings.HasPrefix(base, "http://") {
		return "ws://" + strings.TrimPrefix(base, "http://")
	}
	return base
}
