// Package metrics is Anchor's Prometheus instrumentation.
//
// The measurements here are the ones the project is judged on: scheduling
// latency, step duration, queue depth, recovery behaviour, and spend. They are
// histograms rather than averages wherever a tail matters, because an average
// hides the slow requests that users actually experience -- the whole reason
// p99 is quoted instead of a mean.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds every collector, so a worker or server is handed one object
// rather than reaching for package-level globals. Globals make two instances in
// one test process impossible, which is exactly what the tests need.
type Metrics struct {
	reg *prometheus.Registry

	// SchedulingLatency is claim -> first step: how long a run waited between
	// being picked up and actually starting work. This is the number reported
	// as "p99 scheduling latency".
	SchedulingLatency prometheus.Histogram
	// QueueWait is submit -> claim: how long a run sat in the queue. Distinct
	// from scheduling latency, and the one that grows first under load.
	QueueWait prometheus.Histogram
	// StepDuration is per-step execution time, split by kind.
	StepDuration *prometheus.HistogramVec

	// QueueDepth is how many runs are waiting. The gauge the submit endpoint's
	// high-water mark is compared against.
	QueueDepth prometheus.Gauge
	// ActiveLeases is how many runs are currently owned by a worker.
	ActiveLeases prometheus.Gauge

	// RunsStarted counts starts, split by whether it was a resume. A rising
	// resume count means workers are dying.
	RunsStarted *prometheus.CounterVec
	// RunsFinished counts terminal runs by status.
	RunsFinished *prometheus.CounterVec
	// StepsFailed counts step failures, split by retryable.
	StepsFailed *prometheus.CounterVec
	// SeqConflicts counts lost races for a run's next seq -- two workers
	// believing they owned the same run. Should be near zero; a sustained
	// non-zero rate means leases are expiring while work is still in flight.
	SeqConflicts prometheus.Counter
	// LeasesReaped counts runs recovered from a dead worker.
	LeasesReaped prometheus.Counter

	// TokensUsed and MicroDollarsSpent are the cost counters. Spend is in
	// micro-dollars for the same reason the database is: cents round a single
	// call to zero.
	TokensUsed        *prometheus.CounterVec
	MicroDollarsSpent *prometheus.CounterVec

	// RateLimited counts runs deferred because a token bucket was empty.
	RateLimited *prometheus.CounterVec
	// BreakerState is 0 closed, 1 half-open, 2 open, per provider.
	BreakerState *prometheus.GaugeVec
	// BreakerRejections counts calls refused by an open breaker -- money and
	// time saved by not making a doomed call.
	BreakerRejections *prometheus.CounterVec

	// SubmitsRejected counts 429s returned by the API.
	SubmitsRejected prometheus.Counter
}

// latencyBuckets span a millisecond to half a minute. The boundaries are
// chosen so that both a fast claim and a lease-expiry recovery land in a
// meaningful bucket rather than both in +Inf.
var latencyBuckets = []float64{
	.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30,
}

// New builds the collectors and registers them.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg}

	m.SchedulingLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "anchor", Name: "scheduling_latency_seconds",
		Help:    "Time from a worker claiming a run to its first step starting.",
		Buckets: latencyBuckets,
	})
	m.QueueWait = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "anchor", Name: "queue_wait_seconds",
		Help:    "Time from a run being submitted to being claimed.",
		Buckets: latencyBuckets,
	})
	m.StepDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "anchor", Name: "step_duration_seconds",
		Help:    "Per-step execution time.",
		Buckets: latencyBuckets,
	}, []string{"kind"})

	m.QueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "anchor", Name: "queue_depth",
		Help: "Runs waiting to be claimed.",
	})
	m.ActiveLeases = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "anchor", Name: "active_leases",
		Help: "Runs currently owned by a worker.",
	})

	m.RunsStarted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "anchor", Name: "runs_started_total",
		Help: "Run starts, labelled by whether the run was resumed.",
	}, []string{"resumed"})
	m.RunsFinished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "anchor", Name: "runs_finished_total",
		Help: "Runs reaching a terminal state, by status.",
	}, []string{"status"})
	m.StepsFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "anchor", Name: "steps_failed_total",
		Help: "Step failures, by kind and whether they were retryable.",
	}, []string{"kind", "retryable"})
	m.SeqConflicts = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "anchor", Name: "seq_conflicts_total",
		Help: "Appends that lost the race for a run's next seq.",
	})
	m.LeasesReaped = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "anchor", Name: "leases_reaped_total",
		Help: "Runs requeued after their owner's lease expired.",
	})

	m.TokensUsed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "anchor", Name: "tokens_used_total",
		Help: "Model tokens consumed, by model and direction.",
	}, []string{"model", "direction"})
	m.MicroDollarsSpent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "anchor", Name: "micro_dollars_spent_total",
		Help: "Spend in micro-dollars (1e-6 USD), by model.",
	}, []string{"model"})

	m.RateLimited = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "anchor", Name: "rate_limited_total",
		Help: "Runs deferred because a token bucket was empty.",
	}, []string{"scope"})
	m.BreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "anchor", Name: "breaker_state",
		Help: "Circuit breaker state: 0 closed, 1 half-open, 2 open.",
	}, []string{"provider"})
	m.BreakerRejections = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "anchor", Name: "breaker_rejections_total",
		Help: "Calls refused by an open circuit breaker.",
	}, []string{"provider"})

	m.SubmitsRejected = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "anchor", Name: "submits_rejected_total",
		Help: "Submissions refused with HTTP 429 because the queue was saturated.",
	})

	m.initFixedLabels()

	reg.MustRegister(
		m.SchedulingLatency, m.QueueWait, m.StepDuration,
		m.QueueDepth, m.ActiveLeases,
		m.RunsStarted, m.RunsFinished, m.StepsFailed, m.SeqConflicts, m.LeasesReaped,
		m.TokensUsed, m.MicroDollarsSpent,
		m.RateLimited, m.BreakerState, m.BreakerRejections,
		m.SubmitsRejected,
	)
	return m
}

// initFixedLabels creates a zero-valued series for every label combination
// whose values are known in advance.
//
// A Prometheus *Vec exposes NOTHING until a label combination is first
// observed. Without this, a dashboard panel for "failed runs" reads "no data"
// until the first failure -- which looks identical to a broken scrape, and is
// exactly when someone is staring at the dashboard trying to tell the
// difference.
//
// Only fixed-cardinality labels are pre-created. Model and provider names are
// open sets, so those series necessarily appear on first use; pre-creating them
// would mean guessing every model anyone might ever run.
func (m *Metrics) initFixedLabels() {
	for _, kind := range []string{"model", "tool"} {
		m.StepDuration.WithLabelValues(kind)
		for _, retryable := range []string{"true", "false"} {
			m.StepsFailed.WithLabelValues(kind, retryable)
		}
	}
	for _, status := range []string{"completed", "failed", "cancelled", "needs_review"} {
		m.RunsFinished.WithLabelValues(status)
	}
	for _, resumed := range []string{"true", "false"} {
		m.RunsStarted.WithLabelValues(resumed)
	}
	for _, scope := range []string{"tenant_rate_limit", "provider_rate_limit", "breaker_open"} {
		m.RateLimited.WithLabelValues(scope)
	}
}

// InitProvider creates the zero-valued series for one provider, so breaker
// panels read zero rather than "no data" before the first call is made.
func (m *Metrics) InitProvider(provider string) {
	m.BreakerState.WithLabelValues(provider)
	m.BreakerRejections.WithLabelValues(provider)
}

// Registry exposes the registry so a server can serve /metrics from it.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// ObserveBreakerState records a breaker's position as a number, because
// Prometheus gauges hold numbers and the state is a word.
func (m *Metrics) ObserveBreakerState(provider, state string) {
	var v float64
	switch state {
	case "half-open":
		v = 1
	case "open":
		v = 2
	default:
		v = 0
	}
	m.BreakerState.WithLabelValues(provider).Set(v)
}
