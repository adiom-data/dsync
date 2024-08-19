// Code generated by mockery v2.41.0. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// Runner is an autogenerated mock type for the Runner type
type Runner struct {
	mock.Mock
}

// Run provides a mock function with given fields:
func (_m *Runner) Run() {
	_m.Called()
}

// Setup provides a mock function with given fields: ctx
func (_m *Runner) Setup(ctx context.Context) error {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for Setup")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Teardown provides a mock function with given fields:
func (_m *Runner) Teardown() {
	_m.Called()
}

// NewRunner creates a new instance of Runner. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewRunner(t interface {
	mock.TestingT
	Cleanup(func())
}) *Runner {
	mock := &Runner{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}