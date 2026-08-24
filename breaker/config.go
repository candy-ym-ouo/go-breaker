package breaker

import (
	"fmt"
	"time"
)

type Config struct {
	WindowSize, ProbeCount, SuccessThreshold, MaxConcurrency, MetricSnapshotSec int
	BucketDuration, SleepWindow, AcquireTimeout, CallTimeout                    time.Duration
	ErrorThreshold                                                              float64
	MinRequests                                                                 int64
	Fallback                                                                    Fallback
	EnableResultEvent                                                           bool
}

func DefaultConfig() Config {
	return Config{
		WindowSize:        10,
		BucketDuration:    time.Second,
		ErrorThreshold:    0.5,
		MinRequests:       5,
		SleepWindow:       5 * time.Second,
		ProbeCount:        1,
		SuccessThreshold:  1,
		MaxConcurrency:    100,
		AcquireTimeout:    0,
		CallTimeout:       3 * time.Second,
		Fallback:          Fallback{Type: FallbackReturnErr},
		EnableResultEvent: false,
		MetricSnapshotSec: 5,
	}
}

func (c Config) Validate() error {
	if c.WindowSize < 1 || c.WindowSize > 3600 || c.BucketDuration <= 0 {
		return fmt.Errorf("%w: window size and bucket duration are invalid", ErrConfigInvalid)
	}
	if c.ErrorThreshold < 0 || c.ErrorThreshold > 1 || c.MinRequests < 0 {
		return fmt.Errorf("%w: error threshold or minimum requests is invalid", ErrConfigInvalid)
	}
	if c.SleepWindow <= 0 || c.ProbeCount < 1 || c.SuccessThreshold < 1 || c.SuccessThreshold > c.ProbeCount {
		return fmt.Errorf("%w: sleep window or probe settings are invalid", ErrConfigInvalid)
	}
	if c.MaxConcurrency < 1 || c.AcquireTimeout < 0 || c.CallTimeout <= 0 {
		return fmt.Errorf("%w: concurrency or timeout settings are invalid", ErrConfigInvalid)
	}
	if c.MetricSnapshotSec < 0 {
		return fmt.Errorf("%w: metric snapshot interval cannot be negative", ErrConfigInvalid)
	}
	if c.Fallback.Type < FallbackReturnErr || c.Fallback.Type > FallbackCustomFunc {
		return fmt.Errorf("%w: unknown fallback type", ErrConfigInvalid)
	}
	if c.Fallback.Type == FallbackCustomFunc && c.Fallback.Func == nil {
		return fmt.Errorf("%w: custom fallback requires a function", ErrConfigInvalid)
	}
	return nil
}
func cloneConfig(c Config) *Config { return &c }
