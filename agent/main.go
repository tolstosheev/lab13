package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"

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

var processedTasks int

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

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url)
	if err != nil {
		log.Fatalf("NATS connection error: %v", err)
	}
	defer nc.Close()

	_, err = nc.QueueSubscribe("tasks.risk_assessment", "risk_assessors", func(m *nats.Msg) {
		var req RiskRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			log.Printf("ERROR: failed to unmarshal request: %v", err)
			return
		}

		log.Printf("INFO: processing risk assessment for transaction: %s", req.TransactionID)
		res := calculateRisk(req)

		data, err := json.Marshal(res)
		if err != nil {
			log.Printf("ERROR: failed to marshal response: %v", err)
			return
		}

		if err := nc.Publish("tasks.completed", data); err != nil {
			log.Printf("ERROR: failed to publish result: %v", err)
			return
		}
		processedTasks++
		log.Printf("INFO: risk assessment completed for %s: score %d, verdict %s (processed: %d)", res.TransactionID, res.RiskScore, res.Verdict, processedTasks)
	})

	if err != nil {
		log.Fatalf("Subscription error: %v", err)
	}

	log.Println("INFO: Risk Assessor agent is running...")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan

	log.Printf("INFO: Agent shutting down. Total tasks processed: %d", processedTasks)
}
