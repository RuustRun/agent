package docker

// Volume disk-usage measurement for stateful (database) Eggs, so the dashboard can
// show how full an Egg's persistent volume is. Measured by running du inside the
// container against each mounted named volume. Stateless web Eggs have no volume and
// report zero. British English. No em dashes.

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/docker/docker/api/types/mount"
)

// PersistentVolumePrefix is the naming convention for an Egg's persistent volume
// (ruust-vol-<blobId>, set by the control plane). We measure only these, not the
// anonymous volumes an image's own VOLUME instruction creates (a stateless web Egg's
// image can declare one), so a web Egg reports zero and a database Egg reports the
// real data size.
const PersistentVolumePrefix = "ruust-vol-"

// DiskUsager measures the on-disk usage of a container's persistent volumes. Kept
// separate from the core Client so reconcile and its fake need not know about it; the
// real engineClient implements it and the agent type-asserts to it.
type DiskUsager interface {
	VolumeUsageBytes(ctx context.Context, containerID string) (int64, error)
}

// VolumeUsageBytes sums the on-disk size of every Ruust persistent volume mounted into
// the container, by running du inside it. Returns 0 for a container with no such
// volume (a stateless web Egg). Uses `du -sk` (kilobytes), which both GNU and busybox
// du support, and scales to bytes, so it works on Alpine and Debian based images alike.
func (e *engineClient) VolumeUsageBytes(ctx context.Context, containerID string) (int64, error) {
	info, err := e.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return 0, fmt.Errorf("inspect: %w", err)
	}
	var total int64
	for _, m := range info.Mounts {
		// Only our named persistent volumes, never an image's anonymous VOLUME.
		if m.Type != mount.TypeVolume || m.Destination == "" || !strings.HasPrefix(m.Name, PersistentVolumePrefix) {
			continue
		}
		var out bytes.Buffer
		// -s summary, -k kilobytes, -x stay on the volume's own filesystem.
		if err := e.execCapture(ctx, containerID, []string{"du", "-skx", m.Destination}, nil, nil, &out); err != nil {
			return 0, fmt.Errorf("du %s: %w", m.Destination, err)
		}
		kb, perr := parseLeadingInt(out.String())
		if perr != nil {
			return 0, fmt.Errorf("parsing du output for %s: %w", m.Destination, perr)
		}
		total += kb * 1024
	}
	return total, nil
}

// parseLeadingInt reads the first whitespace-delimited integer from du's output
// ("<size>\t<path>").
func parseLeadingInt(s string) (int64, error) {
	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) == 0 {
		return 0, fmt.Errorf("empty du output")
	}
	return strconv.ParseInt(fields[0], 10, 64)
}
