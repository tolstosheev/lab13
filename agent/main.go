package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/nats-io/nats.go"
)

type Marker struct {
	ID         string  `json:"id"`
	Confidence float64 `json:"confidence"`
}

type RiskRequest struct {
	TransactionID string   `json:"transaction_id"`
	Markers       []Marker `json:"markers"`
}

type RiskResponse struct {
	TransactionID string `json:"transaction_id"`
	RiskScore     int    `json:"risk_score"`
	Verdict       string `json:"verdict"`
	Reason        string `json:"reason"`
}

var errDecode = errors.New("decode error")

var (
	processedTasks int
	mu             sync.Mutex
)

var weights = map[string]float64{
	"BLACKLIST_HIT":     80.0,
	"IMPOSSIBLE_TRAVEL": 50.0,
	"VELOCITY_ATTACK":   30.0,
}

func calculateRisk(req RiskRequest) RiskResponse {
	var totalScore float64
	var reasons []string

	for _, m := range req.Markers {
		if weight, ok := weights[m.ID]; ok {
			contribution := weight * m.Confidence
			totalScore += contribution
			reasons = append(reasons, fmt.Sprintf("%s(%.2f)", m.ID, contribution))
		}
	}

	finalScore := int(math.Max(0, math.Min(100, math.Round(totalScore))))
	verdict := "LOW"
	if finalScore > 80 {
		verdict = "HIGH"
	} else if finalScore > 30 {
		verdict = "MEDIUM"
	}

	return RiskResponse{
		TransactionID: req.TransactionID,
		RiskScore:     finalScore,
		Verdict:       verdict,
		Reason:        fmt.Sprintf("Score %d based on: %v", finalScore, reasons),
	}
}

func processMessage(data []byte) ([]byte, error) {
	var req RiskRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("%w: %v", errDecode, err)
	}
	res := calculateRisk(req)
	return json.Marshal(res)
}

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	agentID := os.Getenv("AGENT_ID")
	if agentID == "" {
		agentID = "agent"
	}

	var agentLog *log.Logger
	logFile, err := os.OpenFile("agent.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		agentLog = log.New(os.Stdout, "["+agentID+"] ", log.LstdFlags)
		agentLog.Printf("WARNING: failed to open log file, stdout only: %v", err)
	} else {
		multiWriter := io.MultiWriter(os.Stdout, logFile)
		agentLog = log.New(multiWriter, "["+agentID+"] ", log.LstdFlags)
		defer logFile.Close()
	}

	agentLog.Printf("INFO: Agent ID: %s", agentID)

	nc, err := nats.Connect(url)
	if err != nil {
		agentLog.Printf("FATAL: NATS connection error: %v", err)
		os.Exit(1)
	}
	defer nc.Close()

	sub, err := nc.QueueSubscribe("tasks.risk_assessment", "risk_assessors", func(m *nats.Msg) {
		resp, err := processMessage(m.Data)
		if err != nil {
			if errors.Is(err, errDecode) {
				agentLog.Printf("DEBUG: failed to decode message: %v", err)
			} else {
				agentLog.Printf("ERROR: failed to process message: %v", err)
			}
			return
		}
		if err := nc.Publish("tasks.completed", resp); err != nil {
			agentLog.Printf("ERROR: failed to publish result: %v", err)
			return
		}
		mu.Lock()
		processedTasks++
		count := processedTasks
		mu.Unlock()
		agentLog.Printf("INFO: risk assessment completed (processed: %d)", count)
	})

	if err != nil {
		agentLog.Printf("FATAL: Subscription error: %v", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	agentLog.Println("INFO: Risk Assessor agent is running...")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	mu.Lock()
	count := processedTasks
	mu.Unlock()
	agentLog.Printf("INFO: Agent shutting down. Total tasks processed: %d", count)

	if err := nc.Drain(); err != nil {
		agentLog.Printf("WARNING: drain error: %v", err)
	}
}
