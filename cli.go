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
	"errors"
	"flag"
	"fmt"
	"github.com/stealthly/syscol/syscol"
	"os"
)

func main() {
	if err := exec(); err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
}

func exec() error {
	args := os.Args
	if len(args) == 1 {
		handleHelp()
		return errors.New("No command supplied")
	}

	command := args[1]
	commandArgs := args[1:]
	os.Args = commandArgs

	switch command {
	case "help":
		return handleHelp()
	case "scheduler":
		return handleScheduler()
	case "start", "stop":
		return handleStartStop(command == "start")
	case "update":
		return handleUpdate()
	case "status":
		return handleStatus()
	}

	return fmt.Errorf("Unknown command: %s\n", command)
}

func handleHelp() error {
	fmt.Println(`Usage:
  help: show this message
  scheduler: configure scheduler
  start: start framework
  stop: stop framework
  update: update configuration
  status: get current status of cluster
More help you can get from ./cli <command> -h`)
	return nil
}

func handleStatus() error {
	var api string
	flag.StringVar(&api, "api", "", "Binding host:port for http/artifact server. Optional if SM_API env is set.")

	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	response := syscol.NewApiRequest(syscol.Config.Api + "/api/status").Get()
	fmt.Println(response.Message)
	return nil
}

func handleScheduler() error {
	var api string
	var user string
	var logLevel string

	flag.StringVar(&syscol.Config.Master, "master", "", "Mesos Master addresses.")
	flag.StringVar(&api, "api", "", "Binding host:port for http/artifact server. Optional if SM_API env is set.")
	flag.StringVar(&user, "user", "", "Mesos user. Defaults to current system user")
	flag.StringVar(&logLevel, "log.level", syscol.Config.LogLevel, "Log level. trace|debug|info|warn|error|critical. Defaults to info.")
	flag.StringVar(&syscol.Config.FrameworkName, "framework.name", syscol.Config.FrameworkName, "Framework name.")
	flag.StringVar(&syscol.Config.FrameworkRole, "framework.role", syscol.Config.FrameworkRole, "Framework role.")

	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	if err := syscol.InitLogging(logLevel); err != nil {
		return err
	}

	if syscol.Config.Master == "" {
		return errors.New("--master flag is required.")
	}

	return new(syscol.Scheduler).Start()
}

func handleStartStop(start bool) error {
	var api string
	flag.StringVar(&api, "api", "", "Binding host:port for http/artifact server. Optional if SM_API env is set.")

	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	apiMethod := "start"
	if !start {
		apiMethod = "stop"
	}

	request := syscol.NewApiRequest(syscol.Config.Api + "/api/" + apiMethod)
	response := request.Get()

	fmt.Println(response.Message)

	return nil
}

func handleUpdate() error {
	var api string
	flag.StringVar(&api, "api", "", "Binding host:port for http/artifact server. Optional if SM_API env is set.")
	flag.StringVar(&syscol.Config.ProducerProperties, "producer.properties", "", "Producer.properties file name.")
	flag.StringVar(&syscol.Config.Topic, "topic", "", "Topic to produce data to.")
	flag.StringVar(&syscol.Config.Transform, "transform", "", "Transofmation to apply to each metric. none|avro|proto")
	flag.StringVar(&syscol.Config.SchemaRegistryUrl, "schema.registry.url", "", "Avro Schema Registry url for transform=avro")

	flag.Parse()

	if err := resolveApi(api); err != nil {
		return err
	}

	request := syscol.NewApiRequest(syscol.Config.Api + "/api/update")
	request.AddParam("producer.properties", syscol.Config.ProducerProperties)
	request.AddParam("topic", syscol.Config.Topic)
	request.AddParam("transform", syscol.Config.Transform)
	request.AddParam("schema.registry.url", syscol.Config.SchemaRegistryUrl)
	response := request.Get()

	fmt.Println(response.Message)

	return nil
}

func resolveApi(api string) error {
	if api != "" {
		syscol.Config.Api = api
		return nil
	}

	if os.Getenv("SM_API") != "" {
		syscol.Config.Api = os.Getenv("SM_API")
		return nil
	}

	return errors.New("Undefined API url. Please provide either a CLI --api option or SM_API env.")
}
