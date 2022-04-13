package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"Lab2/communication"
	"Lab2/util"

	"github.com/google/uuid"
)

var (
	serverHostPort string
	clientID       string

	genericLogger = log.New(os.Stdout, "", 0)
	errorLogger   = log.New(os.Stdout, "ERROR: ", 0)
)

func Start() {
	genericLogger.Println(welcomeMessage)
	clientID = uuid.NewString()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var (
			result string
			err    error
		)
		args := strings.Fields(line)
		switch args[0] {
		case connectCmd:
			if len(args) != 2 {
				err = fmt.Errorf("%s. %s", badArguments, helpPrompt)
				break
			}
			result, err = handleConnect(args[1])
		case readCmd:
			if len(args) != 2 {
				err = fmt.Errorf("%s. %s", badArguments, helpPrompt)
				break
			}
			result, err = handleRead(args[1])
		case writeCmd:
			argc := len(args)
			if argc == 3 {
				result, err = handleWrite(args[1], args[2])
			} else if argc == 5 {
				result, err = writeWithServerReplicatedWriteDelay(args[1], args[2], args[3], args[4])
			} else {
				err = fmt.Errorf("%s. %s", badArguments, helpPrompt)
			}
		case hCmd:
			fallthrough
		case helpCmd:
			result = helpMessage
		case qCmd:
			fallthrough
		case quitCmd:
			genericLogger.Printf("%s!", goodbye)
			return
		default:
			err = fmt.Errorf("%s %q", unrecognizedCommand, args[0])
		}

		if err != nil {
			errorLogger.Printf("%v", err)
		} else {
			if result != "" {
				genericLogger.Printf("%s", result)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func handleConnect(hostPort string) (string, error) {
	if serverHostPort != "" {
		return "", fmt.Errorf("already connected to %q", serverHostPort)
	}
	if err := util.ValidateHostPort(hostPort); err != nil {
		return "", err
	}
	serverHostPort = hostPort

	req, _ := json.Marshal(communication.ClientConnectRequest{
		Op: communication.Connect,
		Args: communication.ClientConnectRequestArgs{
			ClientId: clientID,
		},
	})

	// establish a tcp connection
	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.Dial("tcp", serverHostPort)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = conn.Close()
	}()
	if _, err := conn.Write(req); err != nil {
		return "", err
	}

	var resp communication.ClientConnectResponse
	d := json.NewDecoder(conn)
	if err := d.Decode(&resp); err != nil {
		return "", err
	}
	switch resp.Result {
	case communication.Success:
		return fmt.Sprintf("connected to %q", serverHostPort), nil
	case communication.Fail:
		return "", fmt.Errorf(resp.DetailedResult)
	default:
		return "", fmt.Errorf("unknown operation result from server")
	}
}

func handleRead(key string) (string, error) {
	req, _ := json.Marshal(communication.ClientReadRequest{
		Op: communication.Read,
		Args: communication.ClientReadRequestArgs{
			ClientId: clientID,
			Key:      key,
		},
	})

	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.Dial("tcp", serverHostPort)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = conn.Close()
	}()
	if _, err := conn.Write(req); err != nil {
		return "", err
	}

	// get response from server
	var resp communication.ClientReadResponse
	d := json.NewDecoder(conn)
	if err := d.Decode(&resp); err != nil {
		return "", err
	}
	switch resp.Result {
	case communication.Success:
		return fmt.Sprintf("%q -> %q", resp.Key, resp.Value), nil
	case communication.Fail:
		return "", fmt.Errorf(resp.DetailedResult)
	default:
		return "", fmt.Errorf("unknown operation result from server")
	}
}

func handleWrite(key, value string) (string, error) {
	return write(key, value, "", 0)
}

// writeWithServerReplicatedWriteDelay simulates network delay of ServerReplicatedWrite
// it is for testing purpose to show causal consistency of the system
func writeWithServerReplicatedWriteDelay(key, value, delayHostPort, delay string) (string, error) {
	if err := util.ValidateHostPort(delayHostPort); err != nil {
		return "", err
	}
	delayInSeconds, err := strconv.ParseInt(delay, 10, 64)
	if err != nil || delayInSeconds < 0 {
		return "", err
	}
	return write(key, value, delayHostPort, delayInSeconds)
}

func write(key, value, delayHostPort string, delayInSeconds int64) (string, error) {
	req, _ := json.Marshal(communication.ClientWriteRequest{
		Op: communication.Write,
		Args: communication.ClientWriteRequestArgs{
			ClientId:                      clientID,
			Key:                           key,
			Value:                         value,
			ReplicatedWriteDelayInSeconds: delayInSeconds,
			ReplicatedWriteDelayServer:    delayHostPort,
		},
	})

	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.Dial("tcp", serverHostPort)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = conn.Close()
	}()
	if _, err := conn.Write(req); err != nil {
		return "", err
	}

	// get response from server
	var resp communication.ClientWriteResponse
	d := json.NewDecoder(conn)
	if err := d.Decode(&resp); err != nil {
		return "", err
	}
	switch resp.Result {
	case communication.Success:
		return fmt.Sprintf("successfully written %q -> %q", resp.Key, resp.Value), nil
	case communication.Fail:
		return "", fmt.Errorf(resp.DetailedResult)
	default:
		return "", fmt.Errorf("unknown operation result from server")
	}
}
