// Copyright (c) 2018 Uber Technologies, Inc.
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

package persistence

import (
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"log"
	"os"
	"testing"
)

type (
	storeManager interface {
		setupWorkflowStore()
		tearDownWorkflowStore()
	}

	// TestBase contains classes that every persistence implementation should implement and test
	TestBase struct {
		ExecutionMgrFactory ExecutionManagerFactory
		HistoryMgr          HistoryManager
		MetadataManager     MetadataManager
		ShardMgr            ShardManager
		ShardInfo           *ShardInfo
		TaskMgr             TaskManager
		TaskIDGenerator     TransferTaskIDGenerator
		VisibilityMgr       VisibilityManager
		WorkflowMgr         ExecutionManager

		suite.Suite
		*require.Assertions
		storeManager
	}
)

func (b *TestBase) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
	b.setupWorkflowStore()
}

func (b *TestBase) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	b.Assertions = require.New(b.T())
}

func (b *TestBase) TearDownSuite() {
	b.tearDownWorkflowStore()
}
