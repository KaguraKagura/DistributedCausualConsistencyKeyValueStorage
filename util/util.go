package util

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
)

const (
	AppName = "Causal Consistency Key-Value Data Center"
)

// StructToPrettyJsonString turns a struct into a pretty json string
func StructToPrettyJsonString(v interface{}) string {
	prettyString, _ := json.MarshalIndent(v, "", "\t")
	return string(prettyString)
}

// ValidateHostPort checks if the string is in valid "host:port" format
func ValidateHostPort(hostPort string) error {
	_, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return err
	}

	portNumber, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	if portNumber < 0 || portNumber > 65535 {
		return fmt.Errorf("tcp port out of range")
	}
	return nil
}
