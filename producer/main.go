package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/susmitpy/stream_analytics_ctr/producer/models"
)

func randomId(prefix string, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const numLetters = len(letters)
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(numLetters)]
	}
	return prefix + "-" + string(b)
}

var campaigns = []string{
	"campaign-1",
	"campaign-2",
	"campaign-3",
}

func main() {
	fmt.Println("Starting synthetic producer")

	// graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	brokers := []string{"localhost:29092"}
	kp := models.NewKafkaProducer(brokers, "impressions", "clicks")
	// ensure writers are closed on exit
	defer kp.Close()

	// generator configuration
	impressionsPerSecond := 5
	clickProbability := 0.25 // probability an impression will generate a click
	maxClickDelaySeconds := 10 // max delay before a click is generated after an impression

	var wg sync.WaitGroup

	ticker := time.NewTicker(time.Second / time.Duration(impressionsPerSecond))
	defer ticker.Stop()

	fmt.Printf("Producing impressions at ~%d/sec, clickProb=%.2f\n", impressionsPerSecond, clickProbability)

	LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		case <-ticker.C:
			impr := models.Impression{
				ImprId: randomId("impr", 8),
				UserId: randomId("user", 6),
				CampaignID: campaigns[rand.Intn(len(campaigns))],
				Ts:     time.Now().UnixMilli(),
			}
			if err := kp.Write(ctx, impr); err != nil {
				fmt.Printf("failed to write impression: %v\n", err)
			}

			// randomly schedule a click for this impression
			if rand.Float64() < clickProbability {
				wg.Add(1)
				go func(im models.Impression) {
					defer wg.Done()
					delay := time.Duration(rand.Intn(maxClickDelaySeconds+1)) * time.Second
					select {
					case <-time.After(delay):
						click := models.Click{
							ClickId: randomId("click", 8),
							ImpreId: im.ImprId,
							UserId:  im.UserId,
							TS:      time.Now().UnixMilli(),
						}
						if err := kp.Write(ctx, click); err != nil {
							fmt.Printf("failed to write click: %v\n", err)
						}
					case <-ctx.Done():
						// on shutdown, don't attempt to write late clicks
						return
					}
				}(impr)
			}
		}
	}

	// wait for any in-flight click writers to finish
	fmt.Println("shutting down generator, waiting for in-flight clicks...")
	wg.Wait()
	fmt.Println("shutdown complete")
}