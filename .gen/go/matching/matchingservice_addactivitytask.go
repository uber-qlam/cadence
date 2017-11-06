// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Code generated by thriftrw v1.6.0. DO NOT EDIT.
// @generated

package matching

import (
	"errors"
	"fmt"
	"strings"

	"github.com/uber/cadence/.gen/go/shared"
	"go.uber.org/thriftrw/wire"
)

type MatchingService_AddActivityTask_Args struct {
	AddRequest *AddActivityTaskRequest `json:"addRequest,omitempty"`
}

func (v *MatchingService_AddActivityTask_Args) ToWire() (wire.Value, error) {
	var (
		fields [1]wire.Field
		i      int = 0
		w      wire.Value
		err    error
	)
	if v.AddRequest != nil {
		w, err = v.AddRequest.ToWire()
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 1, Value: w}
		i++
	}
	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

func _AddActivityTaskRequest_Read(w wire.Value) (*AddActivityTaskRequest, error) {
	var v AddActivityTaskRequest
	err := v.FromWire(w)
	return &v, err
}

func (v *MatchingService_AddActivityTask_Args) FromWire(w wire.Value) error {
	var err error
	for _, field := range w.GetStruct().Fields {
		switch field.ID {
		case 1:
			if field.Value.Type() == wire.TStruct {
				v.AddRequest, err = _AddActivityTaskRequest_Read(field.Value)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (v *MatchingService_AddActivityTask_Args) String() string {
	if v == nil {
		return "<nil>"
	}
	var fields [1]string
	i := 0
	if v.AddRequest != nil {
		fields[i] = fmt.Sprintf("AddRequest: %v", v.AddRequest)
		i++
	}
	return fmt.Sprintf("MatchingService_AddActivityTask_Args{%v}", strings.Join(fields[:i], ", "))
}

func (v *MatchingService_AddActivityTask_Args) Equals(rhs *MatchingService_AddActivityTask_Args) bool {
	if !((v.AddRequest == nil && rhs.AddRequest == nil) || (v.AddRequest != nil && rhs.AddRequest != nil && v.AddRequest.Equals(rhs.AddRequest))) {
		return false
	}
	return true
}

func (v *MatchingService_AddActivityTask_Args) MethodName() string {
	return "AddActivityTask"
}

func (v *MatchingService_AddActivityTask_Args) EnvelopeType() wire.EnvelopeType {
	return wire.Call
}

var MatchingService_AddActivityTask_Helper = struct {
	Args           func(addRequest *AddActivityTaskRequest) *MatchingService_AddActivityTask_Args
	IsException    func(error) bool
	WrapResponse   func(error) (*MatchingService_AddActivityTask_Result, error)
	UnwrapResponse func(*MatchingService_AddActivityTask_Result) error
}{}

func init() {
	MatchingService_AddActivityTask_Helper.Args = func(addRequest *AddActivityTaskRequest) *MatchingService_AddActivityTask_Args {
		return &MatchingService_AddActivityTask_Args{AddRequest: addRequest}
	}
	MatchingService_AddActivityTask_Helper.IsException = func(err error) bool {
		switch err.(type) {
		case *shared.BadRequestError:
			return true
		case *shared.InternalServiceError:
			return true
		case *shared.ServiceBusyError:
			return true
		default:
			return false
		}
	}
	MatchingService_AddActivityTask_Helper.WrapResponse = func(err error) (*MatchingService_AddActivityTask_Result, error) {
		if err == nil {
			return &MatchingService_AddActivityTask_Result{}, nil
		}
		switch e := err.(type) {
		case *shared.BadRequestError:
			if e == nil {
				return nil, errors.New("WrapResponse received non-nil error type with nil value for MatchingService_AddActivityTask_Result.BadRequestError")
			}
			return &MatchingService_AddActivityTask_Result{BadRequestError: e}, nil
		case *shared.InternalServiceError:
			if e == nil {
				return nil, errors.New("WrapResponse received non-nil error type with nil value for MatchingService_AddActivityTask_Result.InternalServiceError")
			}
			return &MatchingService_AddActivityTask_Result{InternalServiceError: e}, nil
		case *shared.ServiceBusyError:
			if e == nil {
				return nil, errors.New("WrapResponse received non-nil error type with nil value for MatchingService_AddActivityTask_Result.ServiceBusyError")
			}
			return &MatchingService_AddActivityTask_Result{ServiceBusyError: e}, nil
		}
		return nil, err
	}
	MatchingService_AddActivityTask_Helper.UnwrapResponse = func(result *MatchingService_AddActivityTask_Result) (err error) {
		if result.BadRequestError != nil {
			err = result.BadRequestError
			return
		}
		if result.InternalServiceError != nil {
			err = result.InternalServiceError
			return
		}
		if result.ServiceBusyError != nil {
			err = result.ServiceBusyError
			return
		}
		return
	}
}

type MatchingService_AddActivityTask_Result struct {
	BadRequestError      *shared.BadRequestError      `json:"badRequestError,omitempty"`
	InternalServiceError *shared.InternalServiceError `json:"internalServiceError,omitempty"`
	ServiceBusyError     *shared.ServiceBusyError     `json:"serviceBusyError,omitempty"`
}

func (v *MatchingService_AddActivityTask_Result) ToWire() (wire.Value, error) {
	var (
		fields [3]wire.Field
		i      int = 0
		w      wire.Value
		err    error
	)
	if v.BadRequestError != nil {
		w, err = v.BadRequestError.ToWire()
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 1, Value: w}
		i++
	}
	if v.InternalServiceError != nil {
		w, err = v.InternalServiceError.ToWire()
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 2, Value: w}
		i++
	}
	if v.ServiceBusyError != nil {
		w, err = v.ServiceBusyError.ToWire()
		if err != nil {
			return w, err
		}
		fields[i] = wire.Field{ID: 3, Value: w}
		i++
	}
	if i > 1 {
		return wire.Value{}, fmt.Errorf("MatchingService_AddActivityTask_Result should have at most one field: got %v fields", i)
	}
	return wire.NewValueStruct(wire.Struct{Fields: fields[:i]}), nil
}

func _BadRequestError_Read(w wire.Value) (*shared.BadRequestError, error) {
	var v shared.BadRequestError
	err := v.FromWire(w)
	return &v, err
}

func _InternalServiceError_Read(w wire.Value) (*shared.InternalServiceError, error) {
	var v shared.InternalServiceError
	err := v.FromWire(w)
	return &v, err
}

func _ServiceBusyError_Read(w wire.Value) (*shared.ServiceBusyError, error) {
	var v shared.ServiceBusyError
	err := v.FromWire(w)
	return &v, err
}

func (v *MatchingService_AddActivityTask_Result) FromWire(w wire.Value) error {
	var err error
	for _, field := range w.GetStruct().Fields {
		switch field.ID {
		case 1:
			if field.Value.Type() == wire.TStruct {
				v.BadRequestError, err = _BadRequestError_Read(field.Value)
				if err != nil {
					return err
				}
			}
		case 2:
			if field.Value.Type() == wire.TStruct {
				v.InternalServiceError, err = _InternalServiceError_Read(field.Value)
				if err != nil {
					return err
				}
			}
		case 3:
			if field.Value.Type() == wire.TStruct {
				v.ServiceBusyError, err = _ServiceBusyError_Read(field.Value)
				if err != nil {
					return err
				}
			}
		}
	}
	count := 0
	if v.BadRequestError != nil {
		count++
	}
	if v.InternalServiceError != nil {
		count++
	}
	if v.ServiceBusyError != nil {
		count++
	}
	if count > 1 {
		return fmt.Errorf("MatchingService_AddActivityTask_Result should have at most one field: got %v fields", count)
	}
	return nil
}

func (v *MatchingService_AddActivityTask_Result) String() string {
	if v == nil {
		return "<nil>"
	}
	var fields [3]string
	i := 0
	if v.BadRequestError != nil {
		fields[i] = fmt.Sprintf("BadRequestError: %v", v.BadRequestError)
		i++
	}
	if v.InternalServiceError != nil {
		fields[i] = fmt.Sprintf("InternalServiceError: %v", v.InternalServiceError)
		i++
	}
	if v.ServiceBusyError != nil {
		fields[i] = fmt.Sprintf("ServiceBusyError: %v", v.ServiceBusyError)
		i++
	}
	return fmt.Sprintf("MatchingService_AddActivityTask_Result{%v}", strings.Join(fields[:i], ", "))
}

func (v *MatchingService_AddActivityTask_Result) Equals(rhs *MatchingService_AddActivityTask_Result) bool {
	if !((v.BadRequestError == nil && rhs.BadRequestError == nil) || (v.BadRequestError != nil && rhs.BadRequestError != nil && v.BadRequestError.Equals(rhs.BadRequestError))) {
		return false
	}
	if !((v.InternalServiceError == nil && rhs.InternalServiceError == nil) || (v.InternalServiceError != nil && rhs.InternalServiceError != nil && v.InternalServiceError.Equals(rhs.InternalServiceError))) {
		return false
	}
	if !((v.ServiceBusyError == nil && rhs.ServiceBusyError == nil) || (v.ServiceBusyError != nil && rhs.ServiceBusyError != nil && v.ServiceBusyError.Equals(rhs.ServiceBusyError))) {
		return false
	}
	return true
}

func (v *MatchingService_AddActivityTask_Result) MethodName() string {
	return "AddActivityTask"
}

func (v *MatchingService_AddActivityTask_Result) EnvelopeType() wire.EnvelopeType {
	return wire.Reply
}
