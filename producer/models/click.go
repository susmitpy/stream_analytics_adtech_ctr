package models

import (
	"encoding/json"
)

type Click struct {
	ClickId string `json:"click_id"`
	ImpreId string `json:"impr_id"`
	UserId string `json:"user_id"`
	TS int64 `json:"ts"`
}

// Interface: Event
func (c Click) Key() []byte { return []byte(c.ClickId) }
func (c Click) Topic() string { return "clicks" }
func (c Click) Value() ([]byte, error) { return json.Marshal(c) }