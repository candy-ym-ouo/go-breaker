package breaker

import "time"

type Option func(*options)

type options struct {
	config    Config
	listeners []Listener
}

func WithConfig(cfg Config) Option { return func(o *options) { o.config = cfg } }

func WithErrorThreshold(v float64) Option { return func(o *options) { o.config.ErrorThreshold = v } }

func WithWindowSize(n int) Option { return func(o *options) { o.config.WindowSize = n } }

func WithBucketDuration(d time.Duration) Option {
	return func(o *options) { o.config.BucketDuration = d }
}

func WithSleepWindow(d time.Duration) Option { return func(o *options) { o.config.SleepWindow = d } }

func WithMinRequests(n int64) Option { return func(o *options) { o.config.MinRequests = n } }

func WithProbeCount(n int) Option { return func(o *options) { o.config.ProbeCount = n } }

func WithSuccessThreshold(n int) Option { return func(o *options) { o.config.SuccessThreshold = n } }

func WithMaxConcurrency(n int) Option { return func(o *options) { o.config.MaxConcurrency = n } }

func WithCallTimeout(d time.Duration) Option { return func(o *options) { o.config.CallTimeout = d } }

func WithAcquireTimeout(d time.Duration) Option {
	return func(o *options) { o.config.AcquireTimeout = d }
}

func WithFallback(fallback Fallback) Option { return func(o *options) { o.config.Fallback = fallback } }

func WithFallbackFunc(fn FallbackFunc) Option {
	return func(o *options) {
		o.config.Fallback = Fallback{Type: FallbackCustomFunc, Func: fn}
	}
}

func WithFallbackValue(value interface{}) Option {
	return func(o *options) {
		o.config.Fallback = Fallback{Type: FallbackDefaultValue, Value: value}
	}
}

func WithRetryPolicy(policy RetryPolicy) Option { return func(o *options) { o.config.Retry = policy } }

func WithEventListener(fn func(Event)) Option {
	return func(o *options) { o.listeners = append(o.listeners, Listener(fn)) }
}

func WithResultEvents(v bool) Option { return func(o *options) { o.config.EnableResultEvent = v } }
