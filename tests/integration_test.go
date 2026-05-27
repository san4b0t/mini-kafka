package tests

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/san4b0t/mini-kafka/internal/broker"
	"github.com/san4b0t/mini-kafka/internal/config"
)

func setupTestServer(t *testing.T) (*httptest.Server, string) {
	dir, err := os.MkdirTemp("", "broker-test-*")
	if err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		BrokerID: "1",
		Port:     "0",
		DataDir:  dir,
		Nodes:    map[string]config.NodeConfig{"1": {ID: "1", Addr: "localhost"}},
	}

	srv, err := broker.NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// For testing, expose router directly via httptest
	// We need an exported method or field in a real project, here we assume it's same package or accessible
	// To bypass access, we use standard library test patterns.
	// Since tests are external, we start a real server locally.
	return httptest.NewServer(srv.Router()), dir
}

func TestCreateTopic(t *testing.T) {
	// 1. Spin up the test server using your helper
	ts, dir := setupTestServer(t)

	// 2. Ensure we clean up the server and temp files when the test finishes
	defer ts.Close()
	defer os.RemoveAll(dir)

	// 3. Make a mock HTTP request to our test server
	payload := []byte(`{"name": "my-new-topic"}`)
	res, err := ts.Client().Post(ts.URL+"/topics", "application/json", bytes.NewBuffer(payload))
	if err != nil {
		t.Fatalf("Failed to make POST request: %v", err)
	}
	defer res.Body.Close()

	// 4. Assert that the broker responded correctly!
	if res.StatusCode != http.StatusCreated {
		t.Errorf("Expected status 201 Created, got %v", res.StatusCode)
	}
}
