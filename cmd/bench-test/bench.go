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

package main

import (
	"github.com/uber/cadence/bench-test/lib"
	"github.com/uber/cadence/bench-test/load"
	"log"
	"time"
)

func main() {
	cfg, err := lib.LoadConfig()
	if err != nil {
		log.Fatalf("error loading config:%v", err.Error())
	}
	log.Printf("Config=%+v\n", cfg)
	switch cfg.Service.Role {
	case "controller":
		newController(cfg).Run()
	case "worker":
		newWorker(cfg).Run()
	case "all":
		go func() {
			//wait for controller creating domain
			time.Sleep(1 * time.Second)
			newWorker(cfg).Run()
		}()
		newController(cfg).Run()

	default:
		log.Fatalf("unknown role name %v", cfg.Service.Role)
	}
}

func newController(cfg *lib.Config) lib.Runnable {
	runnable, err := load.NewController(cfg)
	if err != nil {
		log.Fatalf("error creating controller:%v", err.Error())
	}
	return runnable
}

func newWorker(cfg *lib.Config) lib.Runnable {
	runnable, err := load.NewWorker(cfg)
	if err != nil {
		log.Fatalf("error creating worker:%v", err.Error())
	}
	return runnable
}
