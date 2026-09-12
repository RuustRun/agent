// Package shaping applies per-container network egress rate limits using Linux
// traffic control (tc), entered through the container's network namespace with
// nsenter. It is image-independent: it uses the HOST's tc/nsenter against the
// container's netns (by init PID), so it works regardless of what the customer's
// image contains, and a tenant with dropped capabilities cannot remove it. On
// non-Linux dev builds every call is a no-op.
//
// The host must give the agent the privilege to enter a netns and run tc
// (CAP_SYS_ADMIN + CAP_NET_ADMIN, granted as ambient capabilities on the agent's
// systemd unit) and have iproute2 (tc) + util-linux (nsenter) installed.
//
// British English throughout. No em dashes.
package shaping

// The real implementation lives in shaping_linux.go; a no-op stub in
// shaping_other.go keeps macOS/Windows dev builds compiling. Both expose the same
// package-level Apply/Clear/Available functions.
