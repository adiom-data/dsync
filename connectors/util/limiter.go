package util

import (
	"sync"

	"golang.org/x/time/rate"
)

type NamespaceLimiter interface {
	Get(namespace string) *rate.Limiter
}

type namespaceLimiter struct {
	mut                   sync.RWMutex
	limiters              map[string]*rate.Limiter
	defaultLimiterFactory func() *rate.Limiter
}

func (l *namespaceLimiter) Get(namespace string) *rate.Limiter {
	l.mut.RLock()
	limiter, ok := l.limiters[namespace]
	l.mut.RUnlock()
	if !ok {
		l.mut.Lock()
		limiter2, ok2 := l.limiters[namespace]
		if ok2 {
			l.mut.Unlock()
			return limiter2
		}
		limiter = l.defaultLimiterFactory()
		l.limiters[namespace] = limiter
		l.mut.Unlock()
		return limiter
	}
	return limiter
}

func NewNamespaceLimiter(namespaceToLimit map[string]int, defaultLimit int) *namespaceLimiter {
	limiters := map[string]*rate.Limiter{}
	for k, v := range namespaceToLimit {
		limit := rate.Limit(v)
		if v < 0 {
			limit = rate.Inf
		}
		limiters[k] = rate.NewLimiter(limit, v)
	}
	limit := rate.Limit(defaultLimit)
	if defaultLimit < 0 {
		limit = rate.Inf
	}
	return &namespaceLimiter{
		limiters: limiters,
		defaultLimiterFactory: func() *rate.Limiter {
			return rate.NewLimiter(limit, defaultLimit)
		},
	}
}
