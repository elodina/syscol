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
    "net/http"
    "io/ioutil"
    "encoding/json"
    "fmt"
    "time"
    "github.com/stealthly/siesta"
)

type SlaveMetrics struct {
    SlaveID string
    Hostname string
    Port int
    Timestamp int64
    Metrics map[string]interface{}
}

type MetricsReporter struct {
    slaveID string
    host string
    port int
    reportingInterval time.Duration
    producer *siesta.KafkaProducer
    topic string

    stop chan struct{}
}

func NewMetricsReporter(slaveID string, host string, port int, reportingInterval time.Duration, producer *siesta.KafkaProducer, topic string) *MetricsReporter {
    return &MetricsReporter{
        slaveID: slaveID,
        host: host,
        port: port,
        reportingInterval: reportingInterval,
        producer: producer,
        topic: topic,
        stop: make(chan struct{}),
    }
}

func (mr *MetricsReporter) Start() {
    Logger.Debug("Starting metrics reporter")
    tick := time.NewTicker(mr.reportingInterval)

    go func() {
        for meta := range mr.producer.RecordsMetadata {
            _ = meta
        }
    }() //TODO this should be done better

    for {
        select {
        case <-tick.C: {
            metrics, err := mr.GetMetrics()
            if err != nil {
                Logger.Errorf("Error while getting metrics: %s", err)
            }

            slaveMetrics := &SlaveMetrics{
                SlaveID: mr.slaveID,
                Hostname: mr.host,
                Port: mr.port,
                Timestamp: time.Now().UnixNano(),
                Metrics: metrics,
            }

            metricsBytes, err := json.Marshal(slaveMetrics)
            if err != nil {
                Logger.Errorf("Error while marshalling metrics: %s", err)
            }

            mr.producer.Send(&siesta.ProducerRecord{Topic: mr.topic, Value: string(metricsBytes)})
        }
        case <-mr.stop: break
        }
    }

    tick.Stop()
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