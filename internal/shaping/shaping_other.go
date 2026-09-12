//go:build !linux

package shaping

import "context"

// On non-Linux dev builds (macOS/Windows) there is no netns/tc, so shaping is a
// compile-time no-op. Real enforcement only ever runs on Linux hosts.

func Available() bool { return false }

func Apply(_ context.Context, _ int, _ int64) error { return nil }

func Clear(_ context.Context, _ int) error { return nil }
