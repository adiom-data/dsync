package ds_test

import (
	"context"
	"testing"
	"time"

	"github.com/adiom-data/dsync/pkg/verify/ds"
	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
)

func TestFrontier(t *testing.T) {
	frontier := ds.NewFrontier()

	nodeA := &ds.Node{ID: "a"}
	nodeB := &ds.Node{ID: "b"}
	nodeC := &ds.Node{ID: "c"}
	frontier.Put("a", nodeA, 100)
	frontier.Put("b", nodeB, 101)
	frontier.Put("c", nodeC, 102)
	frontier.Put("c", nodeC, 103)
	frontier.Put("b", nodeB, 104)
	frontier.Put("a", nodeA, 105)

	assert.True(t, frontier.Check("c"))
	assert.True(t, frontier.Check("b"))
	assert.True(t, frontier.Check("a"))

	assert.Empty(t, frontier.Delete(100, 0))
	assert.Equal(t, []*ds.Node{nodeC}, frontier.Delete(103, 0))
	assert.Equal(t, []*ds.Node{nodeB, nodeA}, frontier.Delete(110, 0))
	assert.Empty(t, frontier.Delete(110, 0))
}

func TestVerifier(t *testing.T) {
	ctx := context.Background()
	clock := clock.NewMock()
	clock.Set(time.Now())
	verifier := ds.NewVerifier(ctx, clock, 0, 0, 0)
	assert.Empty(t, verifier.Find(0))

	verifier.Put("a", true, 1, true)
	verifier.Put("b", true, 1, true)
	assert.ElementsMatch(t, []ds.Result{{"a", 1, 0}, {"b", 1, 0}}, verifier.Find(0))

	verifier.Put("a", false, 2, true)
	verifier.Put("b", false, 1, true)
	assert.Equal(t, []ds.Result{{"a", 1, 2}}, verifier.Find(0))

	verifier.Put("a", false, 3, false)
	assert.Empty(t, verifier.Find(0))

	clock.Add(time.Minute)
	verifier.Update(time.Minute, 0)
	assert.Equal(t, []ds.Result{{"a", 1, 3}}, verifier.Find(0))

	verifier.Put("a", false, 0, false)
	assert.Empty(t, verifier.Find(0))

	clock.Add(time.Minute)
	verifier.Update(time.Minute, 0)
	assert.Equal(t, []ds.Result{{"a", 1, 0}}, verifier.Find(0))

	verifier.Put("a", true, 0, false)
	verifier.Put("b", true, 0, false)
	assert.Empty(t, verifier.Find(0))

	clock.Add(time.Minute)
	verifier.Update(time.Minute, 0)
	assert.Equal(t, []ds.Result{{"b", 0, 1}}, verifier.Find(0))
}
