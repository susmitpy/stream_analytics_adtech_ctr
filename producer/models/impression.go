package models

import (
	"encoding/json"
)

type Impression struct {
	ImprId string `json:"impr_id"`
	UserId string `json:"user_id"`
	CampaignID string    `json:"campaign_id"`
	Ts int64 `json:"ts"`
}

// Interface: Event
func (i Impression) Key() []byte { return []byte(i.ImprId) }
func (i Impression) Topic() string { return "impressions" }
func (i Impression) Value() ([]byte, error) { return json.Marshal(i) }
