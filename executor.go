// +build executor

/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mesos/mesos-go/executor"
	"github.com/stealthly/syscol/syscol"
)

var logLevel = flag.String("log.level", "info", "Log level. trace|debug|info|warn|error|critical. Defaults to info.")

func main() {
	flag.Parse()
	err := syscol.InitLogging(*logLevel)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	driverConfig := executor.DriverConfig{
		Executor: new(syscol.Executor),
	}

	driver, err := executor.NewMesosExecutorDriver(driverConfig)
	if err != nil {
		syscol.Logger.Error(err)
		os.Exit(1)
	}

	_, err = driver.Start()
	if err != nil {
		syscol.Logger.Error(err)
		os.Exit(1)
	}
	driver.Join()
}
