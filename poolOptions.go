package gwp

type PoolOption func(options *PoolOptions)

type PoolOptions struct {
	MaxJobs     int
	WarmWorkers int
}

// buildPoolOptions loads all Pool options and initializes PoolOptions with it
func buildPoolOptions(options ...PoolOption) *PoolOptions {
	opts := new(PoolOptions)
	for _, opt := range options {
		opt(opts)
	}
	return opts
}

func WithMaxJobs(maxJobs int) PoolOption {
	return func(options *PoolOptions) {
		options.MaxJobs = maxJobs
	}
}

func WithWarmWorkers(warmWorkers int) PoolOption {
	return func(options *PoolOptions) {
		ww := warmWorkers
		if warmWorkers < 0 {
			ww = 0
		}
		options.WarmWorkers = ww
	}
}
