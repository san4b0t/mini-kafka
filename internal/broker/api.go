package broker

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/san4b0t/mini-kafka/internal/storage"
)

type CreateTopicReq struct {
	Name string `json:"name"`
}

type PublishReq struct {
	Value []byte `json:"value"` // base64 encoded by default in JSON, or raw bytes
}

type CommitReq struct {
	Offset uint64 `json:"offset"`
}

func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(payload)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req CreateTopicReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if req.Name == "" {
		respondError(w, http.StatusBadRequest, "Topic name required")
		return
	}

	_, err := s.createLog(req.Name)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to create topic")
		return
	}

	respondJSON(w, http.StatusCreated, map[string]string{"message": "Topic created", "topic": req.Name})
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")

	// Accept raw bytes directly to save memory/processing for high throughput
	body, err := io.ReadAll(r.Body)
	if err != nil || len(body) == 0 {
		respondError(w, http.StatusBadRequest, "Empty or invalid body")
		return
	}

	log, ok := s.getLog(topic)
	if !ok {
		var err error
		log, err = s.createLog(topic)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "Failed to initialize topic")
			return
		}
	}

	offset, err := log.Append(body)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to append message")
		return
	}

	brokerMetrics.MessagesPublished.Add(1)
	respondJSON(w, http.StatusCreated, map[string]interface{}{"offset": offset})
}

func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	offsetStr := r.URL.Query().Get("offset")
	if offsetStr == "" {
		respondError(w, http.StatusBadRequest, "Offset parameter is required")
		return
	}

	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid offset format")
		return
	}

	log, ok := s.getLog(topic)
	if !ok {
		respondError(w, http.StatusNotFound, "Topic not found")
		return
	}

	msg, err := log.Read(offset)
	if err != nil {
		if err == storage.ErrOffsetNotFound {
			respondError(w, http.StatusNotFound, "Offset not found (end of partition)")
			return
		}
		respondError(w, http.StatusInternalServerError, "Failed to read message")
		return
	}

	brokerMetrics.MessagesConsumed.Add(1)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	w.Write(msg)
}

func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	group := r.PathValue("group")

	var req CommitReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if err := s.offsetMgr.Commit(topic, group, req.Offset); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to commit offset")
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{"message": "Offset committed"})
}

func (s *Server) handleGetCommit(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	group := r.PathValue("group")

	offset := s.offsetMgr.Get(topic, group)
	respondJSON(w, http.StatusOK, map[string]uint64{"offset": offset})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok", "broker_id": s.cfg.BrokerID})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, GetMetrics())
}
