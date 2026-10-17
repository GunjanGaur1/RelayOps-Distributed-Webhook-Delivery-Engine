package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type CreateWebhookRequest struct {
	DestinationURL string         `json:"destination_url"`
	EventType      string         `json:"event_type"`
	Payload        map[string]any `json:"payload"`
}

type Delivery struct {
	ID             string         `json:"id"`
	DestinationURL string         `json:"destination_url"`
	EventType      string         `json:"event_type"`
	Payload        map[string]any `json:"payload"`
	Status         string         `json:"status"`
	Attempts       int            `json:"attempts"`
	LastError      string         `json:"last_error,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

type Server struct {
	mu             sync.RWMutex
	deliveries     map[string]*Delivery
	queue          chan string
	workerCount    int
	maxAttempts    int
	retryBaseDelay time.Duration
	client         *http.Client
	accepted       atomic.Uint64
	delivered      atomic.Uint64
	failed         atomic.Uint64
	retries        atomic.Uint64
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func newServer() *Server {
	workers := envInt("WORKER_COUNT", 4)
	return &Server{
		deliveries:     make(map[string]*Delivery),
		queue:          make(chan string, 500),
		workerCount:    workers,
		maxAttempts:    envInt("MAX_ATTEMPTS", 3),
		retryBaseDelay: time.Duration(envInt("RETRY_BASE_DELAY_MS", 500)) * time.Millisecond,
		client:         &http.Client{Timeout: time.Duration(envInt("REQUEST_TIMEOUT_MS", 5000)) * time.Millisecond},
	}
}

func (s *Server) startWorkers() {
	for i := 0; i < s.workerCount; i++ {
		go s.worker(i + 1)
	}
}

func (s *Server) worker(workerID int) {
	for id := range s.queue {
		s.deliver(id, workerID)
	}
}

func (s *Server) deliver(id string, workerID int) {
	for {
		s.mu.Lock()
		d, ok := s.deliveries[id]
		if !ok {
			s.mu.Unlock()
			return
		}
		d.Status = "processing"
		d.Attempts++
		d.UpdatedAt = time.Now().UTC()
		snapshot := *d
		s.mu.Unlock()

		body, err := json.Marshal(map[string]any{
			"id": snapshot.ID, "event_type": snapshot.EventType, "payload": snapshot.Payload,
		})
		if err == nil {
			var req *http.Request
			req, err = http.NewRequest(http.MethodPost, snapshot.DestinationURL, bytes.NewReader(body))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-RelayOps-Event", snapshot.EventType)
				var resp *http.Response
				resp, err = s.client.Do(req)
				if err == nil {
					resp.Body.Close()
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						err = fmt.Errorf("destination returned HTTP %d", resp.StatusCode)
					}
				}
			}
		}

		s.mu.Lock()
		d = s.deliveries[id]
		if err == nil {
			d.Status = "delivered"
			d.LastError = ""
			d.UpdatedAt = time.Now().UTC()
			s.delivered.Add(1)
			s.mu.Unlock()
			log.Printf("worker=%d delivery=%s status=delivered attempt=%d", workerID, id, snapshot.Attempts)
			return
		}

		d.LastError = err.Error()
		d.UpdatedAt = time.Now().UTC()
		if d.Attempts >= s.maxAttempts {
			d.Status = "failed"
			s.failed.Add(1)
			s.mu.Unlock()
			log.Printf("worker=%d delivery=%s status=failed error=%q", workerID, id, err.Error())
			return
		}
		d.Status = "retrying"
		attempt := d.Attempts
		s.retries.Add(1)
		s.mu.Unlock()

		delay := s.retryBaseDelay * time.Duration(1<<(attempt-1))
		time.Sleep(delay)
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "relayops"})
	})
	mux.HandleFunc("POST /webhooks", s.createDelivery)
	mux.HandleFunc("GET /deliveries", s.listDeliveries)
	mux.HandleFunc("GET /deliveries/{id}", s.getDelivery)
	mux.HandleFunc("POST /deliveries/{id}/replay", s.replayDelivery)
	mux.HandleFunc("GET /metrics", s.metrics)
	return cors(mux)
}

func (s *Server) createDelivery(w http.ResponseWriter, r *http.Request) {
	var req CreateWebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.DestinationURL == "" || (len(req.DestinationURL) < 8) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "destination_url is required"})
		return
	}
	if req.EventType == "" {
		req.EventType = "event.created"
	}
	if req.Payload == nil {
		req.Payload = map[string]any{}
	}
	d := &Delivery{
		ID: uuid.NewString(), DestinationURL: req.DestinationURL,
		EventType: req.EventType, Payload: req.Payload, Status: "queued",
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	s.mu.Lock()
	s.deliveries[d.ID] = d
	s.mu.Unlock()
	select {
	case s.queue <- d.ID:
		s.accepted.Add(1)
		writeJSON(w, http.StatusAccepted, d)
	default:
		s.mu.Lock()
		delete(s.deliveries, d.ID)
		s.mu.Unlock()
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "delivery queue is full"})
	}
}

func (s *Server) listDeliveries(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	out := make([]Delivery, 0, len(s.deliveries))
	for _, d := range s.deliveries {
		out = append(out, *d)
	}
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": out, "count": len(out)})
}

func (s *Server) getDelivery(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	d, ok := s.deliveries[r.PathValue("id")]
	if ok {
		copy := *d
		d = &copy
	}
	s.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "delivery not found"})
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) replayDelivery(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	d, ok := s.deliveries[id]
	if !ok {
		s.mu.Unlock()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "delivery not found"})
		return
	}
	if d.Status != "failed" {
		s.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "only failed deliveries can be replayed"})
		return
	}
	d.Status = "queued"
	d.Attempts = 0
	d.LastError = ""
	d.UpdatedAt = time.Now().UTC()
	s.mu.Unlock()
	select {
	case s.queue <- id:
		writeJSON(w, http.StatusAccepted, map[string]string{"id": id, "status": "queued"})
	default:
		s.mu.Lock()
		d.Status = "failed"
		s.mu.Unlock()
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "delivery queue is full"})
	}
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# TYPE relayops_deliveries_accepted_total counter\nrelayops_deliveries_accepted_total %d\n", s.accepted.Load())
	fmt.Fprintf(w, "# TYPE relayops_deliveries_succeeded_total counter\nrelayops_deliveries_succeeded_total %d\n", s.delivered.Load())
	fmt.Fprintf(w, "# TYPE relayops_deliveries_failed_total counter\nrelayops_deliveries_failed_total %d\n", s.failed.Load())
	fmt.Fprintf(w, "# TYPE relayops_delivery_retries_total counter\nrelayops_delivery_retries_total %d\n", s.retries.Load())
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	server := newServer()
	server.startWorkers()
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("RelayOps API listening on :%s with %d workers", port, server.workerCount)
	log.Fatal(http.ListenAndServe(":"+port, server.routes()))
}
