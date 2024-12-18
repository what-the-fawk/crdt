package main

import (
	"bytes"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Clock int

type Comparison int

const (
	Concurrent Comparison = iota
	LessThan
	GreaterThan
	Equal
)

type Patch struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp Clock  `json:"timestamp"`
}

type Get struct {
	Key string `json:"key"`
}

type Data struct {
	Value     string
	Timestamp Clock
}

type LWWMap struct {
	mu       sync.Mutex
	store    map[string]Data
	clock    Clock
	nodeID   string
	replicas []string
}

func NewLWWMap(nodeID string, replicas []string) *LWWMap {
	return &LWWMap{
		store:    make(map[string]Data),
		clock:    Clock(0),
		nodeID:   nodeID,
		replicas: replicas,
	}
}

func (m *LWWMap) Apply(operations []Patch) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, op := range operations {
		timestamp := m.clock
		// user request
		if op.Timestamp < 0 {
			timestamp = m.clock
		}
		value := Data{
			Value:     op.Value,
			Timestamp: timestamp, // timestamp less than 0 -- user request
		}
		existing, exists := m.store[op.Key]
		if !exists || existing.Timestamp < op.Timestamp {
			m.clock++
			m.store[op.Key] = value
			log.Printf("Node %s applied operation %v", m.nodeID, op)
		} else if existing.Timestamp == op.Timestamp {
			// tie-breaker
			if op.Value > existing.Value {
				m.store[op.Key] = value
				log.Printf("Node %s applied operation %v", m.nodeID, op)
				m.clock++
			}
		}
		m.clock = max(m.clock, op.Timestamp)
	}
}

func (m *LWWMap) Patch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}
	log.Println("New Patch request")
	var operations []Patch
	if err := json.NewDecoder(r.Body).Decode(&operations); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Received %d operations for patch", len(operations))
	m.Apply(operations)
	w.WriteHeader(http.StatusOK)
}

func (m *LWWMap) Get(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	log.Println("New Get request")

	m.mu.Lock()
	defer m.mu.Unlock()

	var key Get
	if err := json.NewDecoder(r.Body).Decode(&key); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if data, exists := m.store[key.Key]; exists {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
		return // good ending
	}

	http.Error(w, "Key not found", http.StatusNotFound)
}

// func (m *LWWMap) broadcast(operations []Patch) {
// 	for _, replica := range m.replicas {
// 		go func(replica string) {
// 			for {
// 				url := "http://" + replica + "/patch"
// 				data, _ := json.Marshal(operations)
// 				resp, err := http.Post(url, "application/json", bytes.NewReader(data))
// 				if err != nil {
// 					log.Printf("Failed to send operations to %s: %v", replica, err)
// 					time.Sleep(2 * time.Second)
// 					continue
// 				}
// 				resp.Body.Close()
// 				if resp.StatusCode == http.StatusOK {
// 					log.Printf("Successfully broadcasted %d operations to %s", len(operations), replica)
// 					break
// 				}
// 			}
// 		}(replica)
// 	}
// }

func (m *LWWMap) selectRandomKeys(k int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if k <= 0 || len(m.store) == 0 {
		return []string{}
	}

	k = min(k, len(m.store))

	keys := make([]string, 0, len(m.store))
	for key := range m.store {
		keys = append(keys, key)
	}

	if k >= len(keys) {
		return keys
	}

	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys[:k]
}

func (m *LWWMap) sync() {
	for {
		time.Sleep(time.Duration(rand.Intn(3)) * time.Second)
		log.Println("Syncing with replicas")
		log.Printf("Current state: %v", m.store)
		selectedKeys := m.selectRandomKeys(5)
		operations := make([]Patch, len(selectedKeys))

		for i, key := range selectedKeys {
			data := m.store[key]
			operations[i] = Patch{
				Key:       key,
				Value:     data.Value,
				Timestamp: data.Timestamp,
			}
		}

		replica := m.replicas[rand.Intn(len(m.replicas))]
		url := "http://" + replica + "/patch"
		data, _ := json.Marshal(operations)
		resp, err := http.Post(url, "application/json", bytes.NewReader(data))
		log.Printf("Sending %d operations to %s", len(operations), replica)
		if err != nil {
			log.Printf("Failed to send operations to %s: %v", replica, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			log.Printf("Successfully sent %d operations to %s", len(operations), replica)
		}
	}
}

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		log.Fatal("NODE_ID environment variable is not set")
	}

	replicas := []string{}
	if os.Getenv("REPLICAS") != "" {
		replicas = append(replicas, strings.Split(os.Getenv("REPLICAS"), ",")...)
	} else {
		log.Fatal("REPLICAS environment variable is not set")
	}

	log.Printf("Node %s is starting with replicas %v", nodeID, replicas)

	lwwMap := NewLWWMap(nodeID, replicas)

	http.HandleFunc("/patch", lwwMap.Patch)
	http.HandleFunc("/getKey", lwwMap.Get)

	go lwwMap.sync()

	log.Printf("Node %s is starting on port 8080", nodeID)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
