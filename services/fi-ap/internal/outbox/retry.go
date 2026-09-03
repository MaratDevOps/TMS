package outbox

import (
	"math"
	"time"
)

func BaseDelay(attemptNumber int, initial, max time.Duration, multiplier float64) time.Duration {
	if attemptNumber < 1 {
		attemptNumber = 1
	}
	exp := math.Pow(multiplier, float64(attemptNumber-1))
	delay := time.Duration(float64(initial) * exp)
	if delay > max {
		return max
	}
	if delay < 0 {
		return max
	}
	return delay
}

func WithJitter(base time.Duration, jitter float64, randFloat func() float64) time.Duration {
	if jitter <= 0 {
		return base
	}
	factor := (1 - jitter) + randFloat()*(2*jitter)
	return time.Duration(float64(base) * factor)
}
