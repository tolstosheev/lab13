package main

import (
	"encoding/json"
	"testing"
)

func TestCalculateRisk(t *testing.T) {
	tests := []struct {
		name        string
		request     RiskRequest
		wantScore   int
		wantVerdict string
	}{
		{
			name: "Low risk - no markers",
			request: RiskRequest{
				TransactionID: "tx-1",
				Markers:       []Marker{},
			},
			wantScore:   0,
			wantVerdict: "LOW",
		},
		{
			name: "Low risk - low confidence",
			request: RiskRequest{
				TransactionID: "tx-2",
				Markers: []Marker{
					{ID: "VELOCITY_ATTACK", Confidence: 0.1},
				},
			},
			wantScore:   3,
			wantVerdict: "LOW",
		},
		{
			name: "Low risk - boundary",
			request: RiskRequest{
				TransactionID: "tx-3",
				Markers: []Marker{
					{ID: "VELOCITY_ATTACK", Confidence: 1.0},
				},
			},
			wantScore:   30,
			wantVerdict: "LOW",
		},
		{
			name: "Medium risk - boundary start",
			request: RiskRequest{
				TransactionID: "tx-4",
				Markers: []Marker{
					{ID: "VELOCITY_ATTACK", Confidence: 1.0},
					{ID: "VELOCITY_ATTACK", Confidence: 0.1},
				},
			},
			wantScore:   33,
			wantVerdict: "MEDIUM",
		},
		{
			name: "Medium risk - combined markers",
			request: RiskRequest{
				TransactionID: "tx-5",
				Markers: []Marker{
					{ID: "VELOCITY_ATTACK", Confidence: 1.0},
					{ID: "IMPOSSIBLE_TRAVEL", Confidence: 0.5},
				},
			},
			wantScore:   55,
			wantVerdict: "MEDIUM",
		},
		{
			name: "Medium risk - boundary end",
			request: RiskRequest{
				TransactionID: "tx-6",
				Markers: []Marker{
					{ID: "BLACKLIST_HIT", Confidence: 1.0},
				},
			},
			wantScore:   80,
			wantVerdict: "MEDIUM",
		},
		{
			name: "High risk - boundary start",
			request: RiskRequest{
				TransactionID: "tx-7",
				Markers: []Marker{
					{ID: "BLACKLIST_HIT", Confidence: 1.0},
					{ID: "VELOCITY_ATTACK", Confidence: 0.1},
				},
			},
			wantScore:   83,
			wantVerdict: "HIGH",
		},
		{
			name: "High risk - multiple critical",
			request: RiskRequest{
				TransactionID: "tx-8",
				Markers: []Marker{
					{ID: "BLACKLIST_HIT", Confidence: 1.0},
					{ID: "IMPOSSIBLE_TRAVEL", Confidence: 0.8},
				},
			},
			wantScore: 100,
			wantVerdict: "HIGH",
		},
		{
			name: "Edge case - unknown marker",
			request: RiskRequest{
				TransactionID: "tx-9",
				Markers: []Marker{
					{ID: "UNKNOWN_MARKER", Confidence: 1.0},
				},
			},
			wantScore:   0,
			wantVerdict: "LOW",
		},
		{
			name: "Edge case - zero confidence",
			request: RiskRequest{
				TransactionID: "tx-10",
				Markers: []Marker{
					{ID: "BLACKLIST_HIT", Confidence: 0.0},
				},
			},
			wantScore:   0,
			wantVerdict: "LOW",
		},
		{
			name: "Edge case - negative confidence",
			request: RiskRequest{
				TransactionID: "tx-11",
				Markers: []Marker{
					{ID: "VELOCITY_ATTACK", Confidence: -1.0},
				},
			},
			wantScore:   0,
			wantVerdict: "LOW",
		},
		{
			name: "Edge case - overflow confidence clamped",
			request: RiskRequest{
				TransactionID: "tx-12",
				Markers: []Marker{
					{ID: "BLACKLIST_HIT", Confidence: 2.0},
				},
			},
			wantScore:   100,
			wantVerdict: "HIGH",
		},
		{
			name: "Edge case - empty transaction ID",
			request: RiskRequest{
				TransactionID: "",
				Markers:       []Marker{},
			},
			wantScore:   0,
			wantVerdict: "LOW",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateRisk(tt.request)
			if got.RiskScore != tt.wantScore {
				t.Errorf("calculateRisk() got score = %v, want %v", got.RiskScore, tt.wantScore)
			}
			if got.Verdict != tt.wantVerdict {
				t.Errorf("calculateRisk() got verdict = %v, want %v", got.Verdict, tt.wantVerdict)
			}
		})
	}
}

func TestProcessMessage(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantScore int
		wantErr   bool
	}{
		{
			name: "valid message with markers",
			input: `{
				"transaction_id": "tx-1",
				"markers": [{"id": "BLACKLIST_HIT", "confidence": 1.0}]
			}`,
			wantScore: 80,
			wantErr:   false,
		},
		{
			name: "valid message without markers",
			input: `{
				"transaction_id": "tx-2",
				"markers": []
			}`,
			wantScore: 0,
			wantErr:   false,
		},
		{
			name: "valid message with multiple markers",
			input: `{
				"transaction_id": "tx-3",
				"markers": [
					{"id": "BLACKLIST_HIT", "confidence": 1.0},
					{"id": "VELOCITY_ATTACK", "confidence": 0.5}
				]
			}`,
			wantScore: 95,
			wantErr:   false,
		},
		{
			name:    "invalid json",
			input:   `{bad json`,
			wantErr: true,
		},
		{
			name:    "empty input",
			input:   ``,
			wantErr: true,
		},
		{
			name: "missing transaction_id",
			input: `{
				"markers": [{"id": "BLACKLIST_HIT", "confidence": 1.0}]
			}`,
			wantScore: 80,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := processMessage([]byte(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Errorf("processMessage() expected error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("processMessage() unexpected error: %v", err)
			}
			var res RiskResponse
			if err := json.Unmarshal(data, &res); err != nil {
				t.Fatalf("processMessage() output invalid JSON: %v", err)
			}
			if res.RiskScore != tt.wantScore {
				t.Errorf("processMessage() got score = %v, want %v", res.RiskScore, tt.wantScore)
			}
		})
	}
}

func TestJSONPipeline(t *testing.T) {
	inputJSON := `{
		"transaction_id": "test-uuid",
		"markers": [
			{"id": "BLACKLIST_HIT", "confidence": 1.0},
			{"id": "VELOCITY_ATTACK", "confidence": 0.5}
		]
	}`

	var req RiskRequest
	if err := json.Unmarshal([]byte(inputJSON), &req); err != nil {
		t.Fatalf("Failed to unmarshal input: %v", err)
	}

	res := calculateRisk(req)

	outputJSON, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var finalRes RiskResponse
	if err := json.Unmarshal(outputJSON, &finalRes); err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	if finalRes.RiskScore != 95 {
		t.Errorf("Pipeline failed: expected score 95, got %d", finalRes.RiskScore)
	}
	if finalRes.Verdict != "HIGH" {
		t.Errorf("Pipeline failed: expected verdict HIGH, got %s", finalRes.Verdict)
	}
}
