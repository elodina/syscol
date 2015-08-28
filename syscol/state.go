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
    "sync"
    "net/http"
    "fmt"
    "io/ioutil"
    "encoding/json"
    "strings"
    "strconv"
)

type State struct {
    master string
    slaves map[string]*SlaveInfo
    updateLock sync.Mutex
}

type SlaveInfo struct {
    ID string
    Hostname string
    Port int
}

func NewSlaveInfo(slaveState map[string]interface{}) (*SlaveInfo, error) {
    id := slaveState["id"].(string)
    hostname := slaveState["hostname"].(string)
    pid := slaveState["pid"].(string)

    pidTokens := strings.Split(pid, ":")
    port, err := strconv.Atoi(pidTokens[len(pidTokens) - 1])
    if err != nil {
        return nil, err
    }

    return &SlaveInfo{
        ID: id,
        Hostname: hostname,
        Port: port,
    }, nil
}

func NewState(master string) *State {
    return &State{
        master: master,
        slaves: make(map[string]*SlaveInfo),
    }
}

func (s *State) GetSlaveInfo(slaveID string) *SlaveInfo {
    s.refreshState()

    return s.slaves[slaveID]
}

func (s *State) refreshState() {
    s.updateLock.Lock()
    defer s.updateLock.Unlock()

    state, err := s.getState()
    if err != nil {
        return
    }

    slaves := state["slaves"].([]interface{})

    s.slaves = make(map[string]*SlaveInfo)
    for _, slaveState := range slaves {
        slave, err := NewSlaveInfo(slaveState.(map[string]interface{}))
        if err != nil {
             Logger.Errorf("Could not parse slave state: %s", err)
        }
        s.slaves[slave.ID] = slave
    }
}

func (s *State) getState() (map[string]interface{}, error) {
    response, err := http.Get(fmt.Sprintf("http://%s/state.json", s.master))
    if err != nil {
        Logger.Errorf("Got error while getting state.json: %s", err)
        return nil, err
    }

    responseBody, err := ioutil.ReadAll(response.Body)
    if err != nil {
        Logger.Errorf("Got error while reading state.json response body: %s", err)
        return nil, err
    }

    state := make(map[string]interface{})
    err = json.Unmarshal(responseBody, &state)
    if err != nil {
        Logger.Errorf("Got error while unmarshalling state.json: %s", err)
        return nil, err
    }

    return state, nil
}