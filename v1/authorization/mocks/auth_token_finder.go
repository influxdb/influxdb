// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/influxdata/influxdb/v2/v1/authorization (interfaces: AuthTokenFinder)

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	influxdb "github.com/influxdata/influxdb/v2"
	reflect "reflect"
)

// MockAuthTokenFinder is a mock of AuthTokenFinder interface
type MockAuthTokenFinder struct {
	ctrl     *gomock.Controller
	recorder *MockAuthTokenFinderMockRecorder
}

// MockAuthTokenFinderMockRecorder is the mock recorder for MockAuthTokenFinder
type MockAuthTokenFinderMockRecorder struct {
	mock *MockAuthTokenFinder
}

// NewMockAuthTokenFinder creates a new mock instance
func NewMockAuthTokenFinder(ctrl *gomock.Controller) *MockAuthTokenFinder {
	mock := &MockAuthTokenFinder{ctrl: ctrl}
	mock.recorder = &MockAuthTokenFinderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockAuthTokenFinder) EXPECT() *MockAuthTokenFinderMockRecorder {
	return m.recorder
}

// FindAuthorizationByToken mocks base method
func (m *MockAuthTokenFinder) FindAuthorizationByToken(arg0 context.Context, arg1 string) (*influxdb.Authorization, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindAuthorizationByToken", arg0, arg1)
	ret0, _ := ret[0].(*influxdb.Authorization)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindAuthorizationByToken indicates an expected call of FindAuthorizationByToken
func (mr *MockAuthTokenFinderMockRecorder) FindAuthorizationByToken(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindAuthorizationByToken", reflect.TypeOf((*MockAuthTokenFinder)(nil).FindAuthorizationByToken), arg0, arg1)
}
