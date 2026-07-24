// Command loadgen drives concurrent orders through the gateway end-to-end
// and reports throughput and saga-completion latency percentiles. It logs in
// once (real JWT), fans out N orders across C workers, and polls each to a
// terminal state — so the numbers are true end-to-end saga latencies, not
// just ingest ack times.
//
// Pair it with `scripts/chaos.sh` (kills payment mid-run): every order still
// reaches a terminal state — confirmed, or cancelled-with-compensation —
// which is the property that matters. No order is ever left stuck.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	gw := flag.String("gateway", "http://127.0.0.1:7000", "gateway base URL")
	direct := flag.Bool("direct", false,
		"benchmark the saga directly against the order service, bypassing the "+
			"edge (the rate limiter is a separately-tested concern — you don't "+
			"throttle your own load test)")
	orderURL := flag.String("order", "http://127.0.0.1:7004",
		"order service URL (used with -direct)")
	n := flag.Int("n", 200, "total orders")
	c := flag.Int("c", 16, "concurrent workers")
	product := flag.String("product", "margherita", "product id to order")
	qty := flag.Int("qty", 1, "qty per order")
	id := flag.String("id", "rahul@orderops.dev", "login id")
	pass := flag.String("password", "demo1234", "login password")
	flag.Parse()

	client := &http.Client{Timeout: 15 * time.Second}

	// base+prefix decide where orders go; in direct mode there is no JWT
	base, prefix, tok := *gw, "/api", ""
	if *direct {
		base, prefix = *orderURL, ""
	} else {
		var err error
		if tok, err = login(client, *gw, *id, *pass); err != nil {
			fmt.Fprintln(os.Stderr, "login:", err)
			os.Exit(1)
		}
	}
	via := *gw + " (edge)"
	if *direct {
		via = *orderURL + " (direct saga)"
	}
	fmt.Printf("loadgen: %d orders, %d workers, %dx %s, via %s\n",
		*n, *c, *qty, *product, via)

	var confirmed, cancelled, failed int64
	lat := make([]time.Duration, 0, *n)
	var latMu sync.Mutex
	reasons := map[string]int{}
	var rMu sync.Mutex
	note := func(msg string) { rMu.Lock(); reasons[msg]++; rMu.Unlock() }

	body := fmt.Sprintf(
		`{"customer":"loadgen","items":[{"product_id":%q,"qty":%d}]}`,
		*product, *qty)

	jobs := make(chan int, *n)
	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *c; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				t0 := time.Now()
				state, err := placeAndAwait(client, base, prefix, tok, body)
				switch {
				case err != nil:
					atomic.AddInt64(&failed, 1)
					note(err.Error())
				case state == "confirmed":
					atomic.AddInt64(&confirmed, 1)
				case state == "cancelled":
					atomic.AddInt64(&cancelled, 1)
				default:
					atomic.AddInt64(&failed, 1)
				}
				if err == nil {
					latMu.Lock()
					lat = append(lat, time.Since(t0))
					latMu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	done := confirmed + cancelled
	fmt.Println("\n──────── results ────────")
	fmt.Printf("elapsed          %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput       %.1f orders/s (end-to-end to terminal)\n",
		float64(done)/elapsed.Seconds())
	fmt.Printf("confirmed        %d\n", confirmed)
	fmt.Printf("cancelled        %d (compensated)\n", cancelled)
	fmt.Printf("failed/timeout   %d\n", failed)
	fmt.Printf("saga latency     p50 %s · p95 %s · p99 %s · max %s\n",
		pctl(lat, 0.50), pctl(lat, 0.95), pctl(lat, 0.99), pctl(lat, 1.0))
	if len(reasons) > 0 {
		fmt.Println("failure reasons:")
		for msg, n := range reasons {
			fmt.Printf("  %3d × %s\n", n, msg)
		}
	}
	if failed > 0 {
		os.Exit(1) // a stuck/failed order is a real failure
	}
}

func login(c *http.Client, gw, id, pass string) (string, error) {
	body := fmt.Sprintf(`{"id":%q,"password":%q}`, id, pass)
	resp, err := c.Post(gw+"/api/auth/login", "application/json",
		bytes.NewReader([]byte(body)))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	var out struct {
		Token string `json:"token"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if out.Token == "" {
		return "", fmt.Errorf("no token")
	}
	return out.Token, nil
}

func placeAndAwait(c *http.Client, base, prefix, tok, body string) (string, error) {
	req, _ := http.NewRequest("POST", base+prefix+"/orders",
		bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 {
		return "", fmt.Errorf("place status %d", resp.StatusCode)
	}
	var created struct {
		ID string `json:"id"`
	}
	json.Unmarshal(raw, &created)

	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		state, err := poll(c, base, prefix, tok, created.ID)
		if err == nil && (state == "confirmed" || state == "cancelled") {
			return state, nil
		}
		time.Sleep(60 * time.Millisecond)
	}
	return "", fmt.Errorf("order %s never reached terminal", created.ID)
}

func poll(c *http.Client, base, prefix, tok, id string) (string, error) {
	req, _ := http.NewRequest("GET", base+prefix+"/orders/"+id, nil)
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		Order struct {
			State string `json:"state"`
		} `json:"order"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	return out.Order.State, nil
}

func pctl(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)-1))
	return sorted[i].Round(time.Millisecond)
}
