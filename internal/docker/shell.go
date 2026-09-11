package docker

import (
	"context"
	"fmt"
	"io"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
)

// ShellResize is an interactive terminal's window size, in character cells.
type ShellResize struct {
	Rows uint
	Cols uint
}

// Shell runs an interactive shell inside a container over a pseudo-terminal, bridging
// stdin from `in` to the shell and the shell's terminal output to `out`, and applying
// window resizes from `resize`. It blocks until the shell exits, `in` reaches EOF, or the
// context is cancelled. With a TTY the exec stream is raw (not multiplexed), so the two
// directions are copied byte-for-byte.
//
// The caller MUST have already authorised access to this specific container; Shell does
// no ownership check of its own. Because docker exec runs inside the container's
// namespaces, the session is confined to the container exactly as the workload itself is.
func (e *engineClient) Shell(ctx context.Context, containerID string, in io.Reader, out io.Writer, resize <-chan ShellResize) error {
	created, err := e.cli.ContainerExecCreate(ctx, containerID, types.ExecConfig{
		// Prefer bash when the image ships it, else a POSIX sh. Both are interactive.
		Cmd:          []string{"/bin/sh", "-c", "if command -v bash >/dev/null 2>&1; then exec bash; else exec sh; fi"},
		Tty:          true,
		AttachStdin:  true,
		AttachStdout: true,
		AttachStderr: true,
		Env:          []string{"TERM=xterm-256color"},
	})
	if err != nil {
		return fmt.Errorf("exec create: %w", err)
	}
	resp, err := e.cli.ContainerExecAttach(ctx, created.ID, types.ExecStartCheck{Tty: true})
	if err != nil {
		return fmt.Errorf("exec attach: %w", err)
	}
	defer resp.Close()

	// Apply terminal resizes until the session ends.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-resize:
				if !ok {
					return
				}
				_ = e.cli.ContainerExecResize(ctx, created.ID, container.ResizeOptions{Height: r.Rows, Width: r.Cols})
			}
		}
	}()

	// Pump client stdin into the shell; on client EOF close the write half so the shell
	// sees EOF and exits.
	go func() {
		_, _ = io.Copy(resp.Conn, in)
		_ = resp.CloseWrite()
	}()

	// Stream the shell's terminal output back to the client until it exits (stream EOF) or
	// the client drops. Either way the session ends.
	_, _ = io.Copy(out, resp.Reader)
	return nil
}
