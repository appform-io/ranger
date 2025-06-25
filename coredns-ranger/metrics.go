package rangerdns

import (
	"github.com/coredns/coredns/plugin"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RangerQueryTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: pluginName,
		Name:      "sync_total",
		Help:      "Counter of successful Ranger syncs",
	})

	RangerQueryFailure = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: pluginName,
		Name:      "sync_failure",
		Help:      "Counter of failed Ranger syncs",
	})

	RangerApiRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: pluginName,
		Name:      "api_total",
		Help:      "Ranger api requests total",
	}, []string{"code", "method", "host"})
)
