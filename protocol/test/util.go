// Copyright (c) 2024. Adiom, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package test

import (
	"testing"
	"time"
)

const (
	// NonBlockingTimeout Default timeout for running a method to assert that the method is non-blocking
	NonBlockingTimeout = 100 * time.Millisecond
	// ReadPlanningTimeout Read planning timeout
	ReadPlanningTimeout = 5 * time.Second
	// FlowCompletionTimeout Flow completion timeout
	FlowCompletionTimeout = 10 * time.Second
	// EventReactionTimeout Event reaction timeout
	EventReactionTimeout = 5 * time.Second
	// DataIntegrityCheckTimeout Data integrity check timeout
	DataIntegrityCheckTimeout = 5 * time.Second
	// MaxMessageCount Max Message Count until we interrupt the flow
	MaxMessageCount = 4
)

// GenericMethod represents a method that takes a receiver, variable arguments, and returns an error
type GenericMethod func(receiver interface{}, args ...interface{}) error

// RunWithTimeout runs a given method with its receiver and arguments, checking if it completes within the given timeout
func RunWithTimeout(t *testing.T, receiver interface{}, method GenericMethod, timeout time.Duration, args ...interface{}) error {
	done := make(chan bool)
	var err error = nil

	go func() {
		err = method(receiver, args...)
		done <- true
	}()

	select {
	case <-done:
		// Method completed within the timeout
	case <-time.After(timeout):
		t.Errorf("Method is blocking for too long (timeout: %s)", timeout)
		t.FailNow()
	}

	return err
}
