// Package testinfra spawns throwaway infrastructure for tests (pattern
// proven in JobHive).
package testinfra

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// RedisAddr returns a Redis to test against: REDIS_ADDR env (CI service
// container) or a spawned tools/redis/redis-server.exe, else skip.
func RedisAddr(t *testing.T) string {
	t.Helper()
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	exe := filepath.Join(repoRoot(t), "tools", "redis", "redis-server.exe")
	if runtime.GOOS != "windows" {
		if p, err := exec.LookPath("redis-server"); err == nil {
			exe = p
		} else {
			t.Skip("no REDIS_ADDR and no redis-server available")
		}
	} else if _, err := os.Stat(exe); err != nil {
		t.Skip("no REDIS_ADDR and tools/redis missing — run scripts/get_redis.ps1")
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	cmd := exec.Command(exe, "--port", fmt.Sprint(port),
		"--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.Dial("tcp", addr); err == nil {
			conn.Close()
			return addr
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("redis did not come up within 5s")
	return ""
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate repo root")
	}
	// internal/platform/testinfra/redis.go -> repo root is three dirs up
	return filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(file))))
}
