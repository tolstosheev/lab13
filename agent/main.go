package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
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

	finalScore := int(math.Min(100, math.Round(totalScore)))
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
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("NATS connection error: %v", err)
	}
	defer nc.Close()

	_, err = nc.QueueSubscribe("tasks.risk_assessment", "risk_assessors", func(m *nats.Msg) {
		var req RiskRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			log.Printf("Error unmarshaling request: %v", err)
			return
		}

		log.Printf("Processing risk assessment for transaction: %s", req.TransactionID)
		res := calculateRisk(req)

		data, err := json.Marshal(res)
		if err != nil {
			log.Printf("Error marshaling response: %v", err)
			return
		}

		if err := nc.Publish("tasks.completed", data); err != nil {
			log.Printf("Error publishing result: %v", err)
			return
		}
		log.Printf("Risk assessment completed for %s: score %d, verdict %s", res.TransactionID, res.RiskScore, res.Verdict)
	})

	if err != nil {
		log.Fatalf("Subscription error: %v", err)
	}

	log.Println("Risk Assessor agent is running...")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
