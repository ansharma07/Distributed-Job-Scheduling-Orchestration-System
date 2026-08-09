package scheduler

import (
	"math"
	"math/rand"
	"time"
)

const (
	// BaseRetryDelay is the initial retry delay (1 second)
	BaseRetryDelay = 1 * time.Second

	// MaxRetryDelay is the maximum retry delay (5 minutes)
	MaxRetryDelay = 5 * time.Minute

	// JitterFactor is the randomization factor (0.0 - 1.0)
	// 0.3 means ±30% randomization
	JitterFactor = 0.3
)

// CalculateRetryDelay calculates the next retry delay using exponential backoff with jitter
// Formula: min(base * 2^retryCount, maxDelay) * (1 ± jitter)
//
// Example progression (with jitter):
// - Retry 1: ~1s  (1 * 2^0 = 1s)
// - Retry 2: ~2s  (1 * 2^1 = 2s)
// - Retry 3: ~4s  (1 * 2^2 = 4s)
// - Retry 4: ~8s  (1 * 2^3 = 8s)
// - Retry 5: ~16s (1 * 2^4 = 16s)
// - Retry 6: ~32s (1 * 2^5 = 32s)
// - Retry 7: ~64s (1 * 2^6 = 64s)
// - Retry 8+: ~300s (capped at MaxRetryDelay)
//
// Jitter prevents thundering herd problem when multiple tasks fail simultaneously
func CalculateRetryDelay(retryCount int32) time.Duration {
	// Calculate exponential backoff: base * 2^retryCount
	exponentialDelay := float64(BaseRetryDelay) * math.Pow(2, float64(retryCount))

	// Cap at maximum delay
	if exponentialDelay > float64(MaxRetryDelay) {
		exponentialDelay = float64(MaxRetryDelay)
	}

	// Add jitter: randomize ±30% to prevent thundering herd
	// Example: if delay is 10s, jitter will be between 7s and 13s
	jitter := 1.0 + (rand.Float64()*2-1)*JitterFactor // Range: [1-jitter, 1+jitter]
	delayWithJitter := exponentialDelay * jitter

	return time.Duration(delayWithJitter)
}

// CalculateNextRetryTime calculates when the task should be retried next
func CalculateNextRetryTime(retryCount int32) time.Time {
	delay := CalculateRetryDelay(retryCount)
	return time.Now().Add(delay)
}

// ShouldRetryNow checks if a task is ready to be retried based on NextRetryAt
func ShouldRetryNow(nextRetryAt time.Time) bool {
	// If NextRetryAt is zero (not set), retry immediately
	if nextRetryAt.IsZero() {
		return true
	}

	// Check if current time is past the retry time
	return time.Now().After(nextRetryAt)
}
