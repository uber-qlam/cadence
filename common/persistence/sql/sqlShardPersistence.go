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

package sql

import (
	"github.com/jmoiron/sqlx"
	"github.com/uber/cadence/common/persistence"
	"fmt"
)

type (
	sqlShardManager struct {
		db *sqlx.DB
	}
)

func NewShardPersistence(username, password, host, port, dbName string) (persistence.ShardManager, error) {
	var db, err = sqlx.Connect("mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, dbName))
	if err != nil {
		return nil, err
	}

	return &sqlShardManager{
		db: db,
	}, nil
}

func (m *sqlShardManager) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

func (m *sqlShardManager) CreateShard(request *persistence.CreateShardRequest) error {
	return nil
}

func (m *sqlShardManager) GetShard(request *persistence.GetShardRequest) (*persistence.GetShardResponse, error) {
	return &persistence.GetShardResponse{&persistence.ShardInfo{}}, nil
}

func (m *sqlShardManager) UpdateShard(request *persistence.UpdateShardRequest) error {
	panic("implement me")
}


