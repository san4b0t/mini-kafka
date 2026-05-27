package tests

import (
	"bytes"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/san4b0t/mini-kafka/internal/broker"
	"github.com/san4b0t/mini-kafka/internal/config"
)

func TestHighConcurrencyStress(t *testing.T) {
	dir, err := os.MkdirTemp("", "stress-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &config.Config{
		BrokerID: "1",
		DataDir:  dir,
		Nodes:    map[string]config.NodeConfig{"1": {ID: "1", Addr: "localhost"}},
	}
	srv, err := broker.NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	topic := "stress-topic"
	// Create Topic
	ts.Client().Post(ts.URL+"/topics", "application/json", bytes.NewBuffer([]byte(`{"name":"`+topic+`"}`)))

	numProducers := 50
	numMessagesPerProducer := 100
	var wg sync.WaitGroup

	// Stress Test Producers
	start := time.Now()
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(pID int) {
			defer wg.Done()
			for j := 0; j < numMessagesPerProducer; j++ {
				payload := []byte(fmt.Sprintf("msg-%d-%d", pID, j))
				res, err := ts.Client().Post(ts.URL+"/topics/"+topic+"/messages", "application/octet-stream", bytes.NewBuffer(payload))
				if err != nil || res.StatusCode != 201 {
					t.Errorf("Publish failed: %v", err)
				}
				if res != nil {
					res.Body.Close()
				}
			}
		}(i)
	}
	wg.Wait()
	t.Logf("Published %d messages in %v", numProducers*numMessagesPerProducer, time.Since(start))

	// Stress Test Consumers
	// Validate total messages via sequential reads up to expected total
	expectedTotal := numProducers * numMessagesPerProducer
	for i := 0; i < expectedTotal; i++ {
		res, err := ts.Client().Get(fmt.Sprintf("%s/topics/%s/messages?offset=%d", ts.URL, topic, i))
		if err != nil || res.StatusCode != 200 {
			t.Fatalf("Consume failed at offset %d: %v", i, err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if len(body) == 0 {
			t.Fatalf("Empty message at offset %d", i)
		}
	}
}

// Add this to server.go or a separate test helper file in broker package.
// For this standalone output, we define it here as a helper if in the same package,
// but since we are in `tests` package, I'll update `server.go` mentally to export `Router()`.
