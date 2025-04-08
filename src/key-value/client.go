package key_value

import (
	"Distributed-Lock-Manager/src/config"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type Client struct {
	mutex            sync.Mutex
	executionTimeout time.Duration
	httpClient       *http.Client
	leaderIndex      int
	nodesAddresses   []string
	nodes            map[string]int // nodeId -> nodeIndex

}

type RequestFactory func(leaderAddress string) *http.Request

type CommandIdProvider func() (string, error)

func NewClient(cfg *config.Config, executionTimeoutMs uint) *Client {
	nodes := make(map[string]int)

	for i := range cfg.NodesAddresses {
		nodes[cfg.NodesIds[i]] = i
	}

	return &Client{
		mutex:            sync.Mutex{},
		executionTimeout: time.Duration(executionTimeoutMs) * time.Millisecond,
		httpClient:       &http.Client{},
		leaderIndex:      0, // Just some start index of node that doesn't really matter
		nodesAddresses:   cfg.NodesAddresses,
		nodes:            nodes,
	}
}

func (client *Client) SetKeyValue(key string, value any) (message *string, err error) {
	provide := func() (string, error) {
		return client.ExecuteSetKeyValue(key, value)
	}

	_, message, err = client.waitForCommandExecution(provide)

	return message, err
}

func (client *Client) CompareAndSetKeyValue(key string, oldValue any, newValue any) (result bool, message *string, err error) {
	provide := func() (string, error) {
		return client.executeCompareAndSetKeyValue(key, oldValue, newValue)
	}

	_, message, err = client.waitForCommandExecution(provide)

	if message != nil || err != nil {
		return false, message, err
	}

	return true, message, nil
}

func (client *Client) DeleteKeyValue(key string) (message *string, err error) {
	provide := func() (string, error) {
		return client.executeDeleteKeyValue(key)
	}

	_, message, err = client.waitForCommandExecution(provide)

	return message, err
}

func (client *Client) GetKeyValue(key string) (value any, message *string, err error) {
	provide := func() (string, error) {
		return client.executeGetKeyValue(key)
	}

	return client.waitForCommandExecution(provide)
}

func (client *Client) waitForCommandExecution(provide CommandIdProvider) (value any, message *string, err error) {
	commandId, err := provide()

	if err != nil {
		return nil, nil, fmt.Errorf("waiting for command execution failed: %s", err)
	}

	for {
		time.Sleep(client.executionTimeout)
		info, err := client.getCommandExecutionInfo(commandId)

		if err != nil {
			return nil, nil, fmt.Errorf("get command execution info error: %s", err)
		}

		// The command has not been executed yet
		if !info.Found {
			continue
		}

		if !info.Success {
			return nil, &info.Message, nil
		}

		return info.Value, nil, nil
	}
}

func (client *Client) getCommandExecutionInfo(commandId string) (CommandExecutionInfo, error) {
	factory := func(leaderAddress string) *http.Request {
		url := fmt.Sprintf("http://%s/command/%s", leaderAddress, commandId)
		req, err := http.NewRequest("GET", url, nil)

		if err != nil {
			panic(fmt.Errorf("failed to create get command execution info request: %s", err))
		}

		return req
	}

	body, err := client.performRequest(factory)

	if err != nil {
		return CommandExecutionInfo{}, fmt.Errorf("failed to perform get command execution info request: %s", err)
	}

	// Parse successful response
	respDto := GetCommandExecutionInfoResponse{}
	if err := json.Unmarshal(body, &respDto); err != nil {
		panic(fmt.Errorf("failed to unmarshal 200 response: %s", err))
	}

	return respDto.Info, nil
}

func (client *Client) ExecuteSetKeyValue(key string, value any) (string, error) {
	requestBody, _ := json.Marshal(SetKeyValueRequest{
		Value: value,
	})

	factory := func(leaderAddress string) *http.Request {
		body := bytes.NewBuffer(requestBody)
		url := fmt.Sprintf("http://%s/key/%s", leaderAddress, key)
		req, err := http.NewRequest("POST", url, body)

		if err != nil {
			panic(fmt.Errorf("failed to create set key request: %s", err))
		}

		return req
	}

	return client.executeCommand(factory)
}

func (client *Client) executeCompareAndSetKeyValue(key string, oldValue any, newValue any) (string, error) {
	requestBody, _ := json.Marshal(CompareAndSetKeyValueRequest{
		OldValue: oldValue,
		NewValue: newValue,
	})

	factory := func(leaderAddress string) *http.Request {
		body := bytes.NewBuffer(requestBody)
		url := fmt.Sprintf("http://%s/key/%s", leaderAddress, key)
		req, err := http.NewRequest("PATCH", url, body)

		if err != nil {
			panic(fmt.Errorf("failed to create CAS key request: %s", err))
		}

		return req
	}

	return client.executeCommand(factory)
}

func (client *Client) executeDeleteKeyValue(key string) (string, error) {
	factory := func(leaderAddress string) *http.Request {
		url := fmt.Sprintf("http://%s/key/%s", leaderAddress, key)
		req, err := http.NewRequest("DELETE", url, nil)

		if err != nil {
			panic(fmt.Errorf("failed to create delete key request: %s", err))
		}

		return req
	}

	return client.executeCommand(factory)
}

func (client *Client) executeGetKeyValue(key string) (string, error) {
	factory := func(leaderAddress string) *http.Request {
		url := fmt.Sprintf("http://%s/key/%s", leaderAddress, key)
		req, err := http.NewRequest("GET", url, nil)

		if err != nil {
			panic(fmt.Errorf("failed to create get key request: %s", err))
		}

		return req
	}

	return client.executeCommand(factory)
}

func (client *Client) executeCommand(createRequest RequestFactory) (string, error) {
	body, err := client.performRequest(createRequest)

	if err != nil {
		return "", fmt.Errorf("failed to execute command: %s", err)
	}

	// Parse successful response
	respDto := CommandResponse{}
	if err := json.Unmarshal(body, &respDto); err != nil {
		return "", fmt.Errorf("failed to unmarshal 200 response: %s", body)
	}

	return respDto.RequestId, nil
}

func (client *Client) performRequest(createRequest RequestFactory) ([]byte, error) {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	rotationCount := 0

	for {
		// Rotation cycled
		if rotationCount >= len(client.nodesAddresses) {
			return nil, fmt.Errorf("cluster is offline")
		}

		leaderAddress := client.nodesAddresses[client.leaderIndex]
		req := createRequest(leaderAddress)

		resp, err := client.httpClient.Do(req)

		if err != nil {
			client.rotateLeaderIndex()
			rotationCount++
			continue
		}

		// Leader is found
		rotationCount = 0

		body, err := io.ReadAll(resp.Body)

		if err != nil {
			return nil, fmt.Errorf("failed to read body: %s", err)
		}

		if err := resp.Body.Close(); err != nil {
			return nil, fmt.Errorf("failed to close body: %s", err)
		}

		// Cluster returned 400
		if resp.StatusCode == http.StatusBadRequest {
			respDto := ErrorResponse{}
			if err := json.Unmarshal(body, &respDto); err != nil {
				return nil, fmt.Errorf("failed to unmarshal 400 response: %s", err)
			}

			return nil, fmt.Errorf("received bad request: %s", respDto.Error)
		}

		// Check fields isLeader and leaderId of response
		respDto := LeaderInfo{}
		if err := json.Unmarshal(body, &respDto); err != nil {
			return nil, fmt.Errorf("failed to unmarshal 200 response: %s", err)
		}

		if respDto.LeaderId == "" {
			return nil, fmt.Errorf("cluster has no leader")
		}

		// Change index of leader
		if !respDto.IsLeader {
			newLeaderId := respDto.LeaderId
			client.leaderIndex = client.nodes[newLeaderId]
			continue
		}

		return body, nil
	}
}

func (client *Client) rotateLeaderIndex() {
	client.leaderIndex = (client.leaderIndex + 1) % len(client.nodesAddresses)
}
