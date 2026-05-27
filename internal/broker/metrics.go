package broker

import (
	"sync/atomic"
)

type Metrics struct {
	MessagesPublished atomic.Uint64
	MessagesConsumed  atomic.Uint64
	TotalTopics       atomic.Uint64
}

var brokerMetrics = &Metrics{}

func GetMetrics() map[string]uint64 {
	return map[string]uint64{
		"messages_published": brokerMetrics.MessagesPublished.Load(),
		"messages_consumed":  brokerMetrics.MessagesConsumed.Load(),
		"total_topics":       brokerMetrics.TotalTopics.Load(),
	}
}
