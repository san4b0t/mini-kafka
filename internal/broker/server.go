package broker

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"sync"

	"github.com/san4b0t/mini-kafka/internal/config"
	"github.com/san4b0t/mini-kafka/internal/storage"
)

type Server struct {
	cfg        *config.Config
	cluster    *ClusterManager
	offsetMgr  *storage.OffsetManager
	logsMu     sync.RWMutex
	logs       map[string]*storage.CommitLog
	router     *http.ServeMux
	proxyCache map[string]*httputil.ReverseProxy
	proxyMu    sync.Mutex
}

func NewServer(cfg *config.Config) (*Server, error) {
	offsetMgr, err := storage.NewOffsetManager(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	s := &Server{
		cfg:        cfg,
		cluster:    NewClusterManager(cfg),
		offsetMgr:  offsetMgr,
		logs:       make(map[string]*storage.CommitLog),
		router:     http.NewServeMux(),
		proxyCache: make(map[string]*httputil.ReverseProxy),
	}

	s.setupRoutes()
	return s, nil
}

func (s *Server) setupRoutes() {
	// Middleware for routing to correct cluster node
	s.router.Handle("POST /topics", s.clusterProxy(http.HandlerFunc(s.handleCreateTopic)))
	s.router.Handle("POST /topics/{topic}/messages", s.clusterProxy(http.HandlerFunc(s.handlePublish)))
	s.router.Handle("GET /topics/{topic}/messages", s.clusterProxy(http.HandlerFunc(s.handleConsume)))
	s.router.Handle("POST /topics/{topic}/groups/{group}/commit", s.clusterProxy(http.HandlerFunc(s.handleCommit)))
	s.router.Handle("GET /topics/{topic}/groups/{group}", s.clusterProxy(http.HandlerFunc(s.handleGetCommit)))

	s.router.HandleFunc("GET /health", s.handleHealth)
	s.router.HandleFunc("GET /metrics", s.handleMetrics)
}

func (s *Server) Serve() error {
	slog.Info("Starting broker", "id", s.cfg.BrokerID, "port", s.cfg.Port)
	return http.ListenAndServe(":"+s.cfg.Port, s.router)
}

// Router exposes the internal ServeMux so external test packages can wrap it in httptest.NewServer.
func (s *Server) Router() *http.ServeMux {
	return s.router
}

func (s *Server) getLog(topic string) (*storage.CommitLog, bool) {
	s.logsMu.RLock()
	defer s.logsMu.RUnlock()
	log, ok := s.logs[topic]
	return log, ok
}

func (s *Server) createLog(topic string) (*storage.CommitLog, error) {
	s.logsMu.Lock()
	defer s.logsMu.Unlock()

	if log, ok := s.logs[topic]; ok {
		return log, nil
	}

	logDir := filepath.Join(s.cfg.DataDir, topic)
	log, err := storage.NewCommitLog(logDir)
	if err != nil {
		return nil, err
	}
	s.logs[topic] = log
	brokerMetrics.TotalTopics.Add(1)
	return log, nil
}

// clusterProxy is an HTTP middleware that intercepts requests, checks topic ownership,
// and proxies the request to the correct broker node if necessary.
func (s *Server) clusterProxy(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")

		// If creating topic, parse body to find topic name
		if topic == "" && r.Method == "POST" && r.URL.Path == "/topics" {
			// We skip proxying creation itself to keep it simple, or force client
			// to connect to the owner. Let's just create it locally for the request
			// and let the client hit the owner.
			next.ServeHTTP(w, r)
			return
		}

		if topic != "" && !s.cluster.IsOwner(topic) {
			owner := s.cluster.GetOwner(topic)
			slog.Info("Proxying request", "topic", topic, "to_owner", owner.ID)
			s.proxyRequest(w, r, owner.Addr)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *Server) proxyRequest(w http.ResponseWriter, r *http.Request, targetAddr string) {
	s.proxyMu.Lock()
	proxy, ok := s.proxyCache[targetAddr]
	if !ok {
		targetURL, _ := url.Parse("http://" + targetAddr)
		proxy = httputil.NewSingleHostReverseProxy(targetURL)
		s.proxyCache[targetAddr] = proxy
	}
	s.proxyMu.Unlock()

	proxy.ServeHTTP(w, r)
}
