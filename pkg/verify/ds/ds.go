package ds

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

func NewVerifier(ctx context.Context, clock clock.Clock, delay time.Duration, poll time.Duration, limit int) *verifier {
	frontier := NewFrontier()
	v := &verifier{
		clock:    clock,
		allNodes: map[string]*Node{},
		mismatch: map[string]struct{}{},
		frontier: frontier,
	}

	if delay > 0 && poll > 0 {
		go func() {
			ticker := clock.Ticker(poll)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					v.Update(delay, limit)
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return v
}

type verifier struct {
	clock    clock.Clock
	allNodes map[string]*Node
	mismatch map[string]struct{}
	frontier *frontierImpl
	mut      sync.Mutex
}

// MismatchCountAndTotal implements Verifier.
func (v *verifier) MismatchCountAndTotal() (int64, int64) {
	v.mut.Lock()
	defer v.mut.Unlock()
	return int64(len(v.mismatch)), int64(len(v.allNodes))
}

func (v *verifier) Update(delay time.Duration, limit int) {
	v.mut.Lock()
	defer v.mut.Unlock()
	items := v.frontier.Delete(v.clock.Now().Add(-delay).UnixMilli(), limit)
	for _, item := range items {
		if item.Left != item.Right {
			v.mismatch[item.ID] = struct{}{}
		} else if item.Left == 0 && item.Right == 0 {
			delete(v.allNodes, item.ID)
		}
	}
}

// Find implements Verifier.
func (v *verifier) Find(limit int) []Result {
	v.mut.Lock()
	defer v.mut.Unlock()
	var res []Result
	for k := range v.mismatch {
		n := v.allNodes[k]
		res = append(res, Result{
			ID:    n.ID,
			Left:  n.Left,
			Right: n.Right,
		})
		if limit != 0 && len(res) >= limit {
			break
		}
	}
	return res
}

// Put implements Verifier.
func (v *verifier) Put(id string, left bool, h uint64, direct bool) {
	v.mut.Lock()
	defer v.mut.Unlock()
	existing, ok := v.allNodes[id]
	if !ok {
		if h == 0 {
			return
		}
		newNode := &Node{
			ID: id,
		}
		if left {
			newNode.Left = h
		} else {
			newNode.Right = h
		}
		v.allNodes[id] = newNode
		if direct {
			v.mismatch[id] = struct{}{}
		} else {
			v.frontier.Put(id, newNode, v.clock.Now().UnixMilli())
		}
		return
	}
	if left {
		existing.Left = h
	} else {
		existing.Right = h
	}
	id = existing.ID
	if direct {
		if existing.Left != existing.Right {
			v.mismatch[id] = struct{}{}
		} else {
			delete(v.mismatch, id)
			if existing.Left == 0 && existing.Right == 0 {
				delete(v.allNodes, id)
			}
		}
	} else {
		v.frontier.Put(id, existing, v.clock.Now().UnixMilli())
		delete(v.mismatch, id)
	}
}

type Node struct {
	ID    string
	Left  uint64
	Right uint64
}

var _ Verifier = &verifier{}
