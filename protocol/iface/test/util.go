package test

import (
	"testing"
	"time"
)

const (
	// Default timeout for running a method to assert that the method is non-blocking
	NonBlockingTimeout = 100 * time.Millisecond
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
	}

	return err
}
