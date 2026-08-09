package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCalculateRetryDelay(t *testing.T) {
	tests := []struct {
		name        string
		retryCount  int32
		minExpected time.Duration
		maxExpected time.Duration
	}{
		{
			name:        "First retry (2^0 = 1s)",
			retryCount:  0,
			minExpected: 700 * time.Millisecond,  // 1s - 30% jitter
			maxExpected: 1300 * time.Millisecond, // 1s + 30% jitter
		},
		{
			name:        "Second retry (2^1 = 2s)",
			retryCount:  1,
			minExpected: 1400 * time.Millisecond, // 2s - 30% jitter
			maxExpected: 2600 * time.Millisecond, // 2s + 30% jitter
		},
		{
			name:        "Third retry (2^2 = 4s)",
			retryCount:  2,
			minExpected: 2800 * time.Millisecond, // 4s - 30% jitter
			maxExpected: 5200 * time.Millisecond, // 4s + 30% jitter
		},
		{
			name:        "Fourth retry (2^3 = 8s)",
			retryCount:  3,
			minExpected: 5600 * time.Millisecond,  // 8s - 30% jitter
			maxExpected: 10400 * time.Millisecond, // 8s + 30% jitter
		},
		{
			name:        "Large retry count (capped at MaxRetryDelay)",
			retryCount:  10,
			minExpected: 3*time.Minute + 30*time.Second, // 5min - 30% jitter
			maxExpected: MaxRetryDelay + 2*time.Minute,  // 5min + 30% jitter
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run multiple times to account for jitter randomness
			for i := 0; i < 10; i++ {
				delay := CalculateRetryDelay(tt.retryCount)
				assert.GreaterOrEqual(t, delay, tt.minExpected,
					"Delay should be >= min expected (with jitter)")
				assert.LessOrEqual(t, delay, tt.maxExpected,
					"Delay should be <= max expected (with jitter)")
			}
		})
	}
}

func TestCalculateRetryDelay_ExponentialGrowth(t *testing.T) {
	// Verify exponential growth pattern
	delay0 := CalculateRetryDelay(0)
	delay1 := CalculateRetryDelay(1)
	delay2 := CalculateRetryDelay(2)
	delay3 := CalculateRetryDelay(3)

	// Each delay should be roughly 2x the previous (accounting for jitter)
	// We use a loose check because of jitter randomness
	assert.Greater(t, delay1, delay0, "Delay should grow exponentially")
	assert.Greater(t, delay2, delay1, "Delay should grow exponentially")
	assert.Greater(t, delay3, delay2, "Delay should grow exponentially")
}

func TestCalculateNextRetryTime(t *testing.T) {
	before := time.Now()
	nextRetry := CalculateNextRetryTime(2) // Should be ~4s from now
	after := time.Now()

	// Next retry should be in the future
	assert.True(t, nextRetry.After(before), "Next retry should be in the future")

	// Should be roughly 4 seconds from now (with jitter: 2.8s - 5.2s)
	expectedMin := before.Add(2800 * time.Millisecond)
	expectedMax := after.Add(5200 * time.Millisecond)

	assert.True(t, nextRetry.After(expectedMin) || nextRetry.Equal(expectedMin),
		"Next retry should be after minimum expected time")
	assert.True(t, nextRetry.Before(expectedMax),
		"Next retry should be before maximum expected time")
}

func TestShouldRetryNow(t *testing.T) {
	tests := []struct {
		name        string
		nextRetryAt time.Time
		expected    bool
	}{
		{
			name:        "Zero time (not set) - should retry immediately",
			nextRetryAt: time.Time{},
			expected:    true,
		},
		{
			name:        "Future time - should not retry yet",
			nextRetryAt: time.Now().Add(10 * time.Second),
			expected:    false,
		},
		{
			name:        "Past time - should retry now",
			nextRetryAt: time.Now().Add(-10 * time.Second),
			expected:    true,
		},
		{
			name:        "Current time - should retry now",
			nextRetryAt: time.Now(),
			expected:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldRetryNow(tt.nextRetryAt)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRetryDelay_NoNegativeValues(t *testing.T) {
	// Test edge cases to ensure no negative delays
	for i := int32(0); i < 20; i++ {
		delay := CalculateRetryDelay(i)
		assert.Greater(t, delay, time.Duration(0),
			"Delay should always be positive for retry count %d", i)
	}
}

func TestRetryDelay_MaxCapEnforced(t *testing.T) {
	// Test that very large retry counts are capped at MaxRetryDelay
	for i := int32(10); i < 100; i++ {
		delay := CalculateRetryDelay(i)
		// With jitter, max should be MaxRetryDelay * (1 + JitterFactor)
		maxAllowed := time.Duration(float64(MaxRetryDelay) * (1 + JitterFactor + 0.01)) // +0.01 for float precision
		assert.LessOrEqual(t, delay, maxAllowed,
			"Delay should be capped at MaxRetryDelay (with jitter) for retry count %d", i)
	}
}
