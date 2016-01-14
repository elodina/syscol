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

package syscol

import (
	"encoding/json"
	"fmt"
	"github.com/stealthly/siesta"
	"github.com/stealthly/syscol/avro"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	TransformNone = "none"
	TransformAvro = "avro"
)

type SlaveMetrics struct {
	SlaveID   string
	Hostname  string
	Port      int32
	Namespace string
	Timestamp int64
	Metrics   map[string]interface{}
}

type MetricsReporter struct {
	slaveID           string
	host              string
	port              int32
	namespace         string
	reportingInterval time.Duration
	producer          *siesta.KafkaProducer
	topic             string
	transform         func(map[string]interface{}) interface{}

	stop chan struct{}
}

func NewMetricsReporter(slaveID string, host string, port int32, namespace string, reportingInterval time.Duration, producer *siesta.KafkaProducer, topic string, transform string) *MetricsReporter {
	reporter := &MetricsReporter{
		slaveID:           slaveID,
		host:              host,
		port:              port,
		namespace:         namespace,
		reportingInterval: reportingInterval,
		producer:          producer,
		topic:             topic,
		stop:              make(chan struct{}),
	}

	reporter.transform = reporter.transformNone
	if transform == TransformAvro {
		reporter.transform = reporter.transformAvro
	}

	return reporter
}

func (mr *MetricsReporter) Start() {
	Logger.Debug("Starting metrics reporter")
	tick := time.NewTicker(mr.reportingInterval)

	go func() {
		for meta := range mr.producer.RecordsMetadata {
			Logger.Tracef("Received record metadata: topic %s, partition %d, offset %d, error %s", meta.Topic, meta.Partition, meta.Offset, meta.Error)
		}
	}()

	for {
		select {
		case <-tick.C:
			{
				metrics, err := mr.GetMetrics()
				if err != nil {
					Logger.Errorf("Error while getting metrics: %s", err)
				}

				slaveMetrics := mr.transform(metrics)

				mr.producer.Send(&siesta.ProducerRecord{Topic: mr.topic, Value: slaveMetrics})
			}
		case <-mr.stop:
			{
				tick.Stop()
				return
			}
		}
	}
}

func (mr *MetricsReporter) Stop() {
	Logger.Debug("Stopping metrics reporter")
	mr.stop <- struct{}{}
}

func (mr *MetricsReporter) GetMetrics() (map[string]interface{}, error) {
	metrics := make(map[string]interface{})

	response, err := http.Get(fmt.Sprintf("http://%s:%d/metrics/snapshot", mr.host, mr.port))
	if err != nil {
		return metrics, err
	}

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return metrics, err
	}

	err = json.Unmarshal(responseBody, &metrics)
	if err != nil {
		return metrics, err
	}

	return metrics, nil
}

func (mr *MetricsReporter) transformNone(metrics map[string]interface{}) interface{} {
	slaveMetrics := &SlaveMetrics{
		SlaveID:   mr.slaveID,
		Hostname:  mr.host,
		Port:      mr.port,
		Namespace: mr.namespace,
		Timestamp: time.Now().UnixNano(),
		Metrics:   metrics,
	}

	metricsBytes, err := json.Marshal(slaveMetrics)
	if err != nil {
		Logger.Errorf("Error while marshalling metrics: %s", err)
	}

	return string(metricsBytes)
}

func (mr *MetricsReporter) transformAvro(metrics map[string]interface{}) interface{} {
	metricsBytes, err := json.Marshal(metrics)
	if err != nil {
		Logger.Errorf("Error while marshalling metrics: %s", err)
	}

	return &avro.SlaveMetrics{
		SlaveID:   mr.slaveID,
		Hostname:  mr.host,
		Port:      mr.port,
		Namespace: mr.namespace,
		Timestamp: time.Now().UnixNano(),
		Metrics:   metricsBytes,
	}
}
