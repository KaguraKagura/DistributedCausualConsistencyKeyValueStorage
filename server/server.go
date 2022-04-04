package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"Lab2/communication"
	"Lab2/util"
)

type genericRequest struct {
	Op   string
	Args json.RawMessage
}

type valueOfKey struct {
	value                 string
	originalServer        string
	lamportClockTimestamp uint64
}

type causalConsistencyMaintainer struct {
	dependencyByClient map[string][]communication.DependencyData
	sync.Mutex
}

type kvStorage struct {
	storage map[string]valueOfKey
	sync.Mutex
}

type lamportsClock struct {
	clock uint64
	sync.Mutex
}

var (
	selfHostPort          string
	otherServersHostPorts []string
	storage               kvStorage
	maintainer            causalConsistencyMaintainer
	clock                 lamportsClock

	genericLogger = log.New(os.Stdout, "", 0)
	infoLogger    = log.New(os.Stdout, "INFO: ", 0)
	errorLogger   = log.New(os.Stdout, "ERROR: ", 0)
)

func Start() {
	genericLogger.Println(welcomeMessage)

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
		case startCmd:
			if len(args) < 2 {
				err = fmt.Errorf("%s. %s", badArguments, helpPrompt)
				break
			}
			err = start(args[1], args[2:])
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

	genericLogger.Printf("%s!", goodbye)
}

func start(hostPort string, otherServers []string) error {
	if selfHostPort != "" {
		return fmt.Errorf("already listening on %q", selfHostPort)
	}

	for _, hp := range append([]string{hostPort}, otherServers...) {
		if err := util.ValidateHostPort(hp); err != nil {
			return fmt.Errorf("bad host:port %q: %w", hp, err)
		}
	}

	selfHostPort = hostPort
	otherServersHostPorts = make([]string, len(otherServers))
	copy(otherServersHostPorts, otherServers)
	storage.storage = make(map[string]valueOfKey)
	maintainer.dependencyByClient = make(map[string][]communication.DependencyData)

	// start to listen
	l, err := net.Listen("tcp", hostPort)
	if err != nil {
		return err
	}
	infoLogger.Printf("server listening on %q", selfHostPort)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				errorLogger.Printf("%v", err)
				continue
			}

			go func() {
				client := conn.RemoteAddr().String()
				// add an empty dependency list for the current client
				maintainer.Lock()
				maintainer.dependencyByClient[client] = make([]communication.DependencyData, 0)
				maintainer.Unlock()

				var resp []byte

				var genericReq genericRequest
				failToUnmarshalResp := makeFailResp("fail to unmarshal")
				d := json.NewDecoder(conn)
				err := d.Decode(&genericReq)
				if err != nil {
					resp = failToUnmarshalResp
				} else {
					switch genericReq.Op {
					case string(communication.Read):
						var temp struct {
							Key string
						}
						if err := json.Unmarshal(genericReq.Args, &temp); err != nil {
							resp = failToUnmarshalResp
							break
						}
						resp = handleClientRead(client, communication.ClientReadRequest{
							Op:  communication.ClientServerOperation(genericReq.Op),
							Key: temp.Key,
						})
					case string(communication.Write):
						var temp struct {
							Key   string
							Value string
						}
						if err := json.Unmarshal(genericReq.Args, &temp); err != nil {
							resp = failToUnmarshalResp
							break
						}
						resp = handleClientWrite(client, communication.ClientWriteRequest{
							Op:    communication.ClientServerOperation(genericReq.Op),
							Key:   temp.Key,
							Value: temp.Value,
						})
					case string(communication.ReplicatedWrite):
						var temp struct {
							Key            string
							Value          string
							Dependencies   []communication.DependencyData
							Client         string
							OriginalServer string
							Clock          uint64
						}
						if err := json.Unmarshal(genericReq.Args, &temp); err != nil {
							resp = failToUnmarshalResp
							break
						}
						go handleServerReplicatedWrite(communication.ServerReplicatedWriteRequest{
							Op:             communication.ServerServerOperation(genericReq.Op),
							Key:            temp.Key,
							Value:          temp.Value,
							Dependencies:   temp.Dependencies,
							Client:         temp.Client,
							OriginalServer: temp.OriginalServer,
							Clock:          temp.Clock,
						})
					default:
						resp = makeFailResp(fmt.Sprintf("unknown operation %q", genericReq.Op))
					}
				}

				if resp != nil {
					if _, err := conn.Write(resp); err != nil {
						errorLogger.Printf("%v", err)
					}
				}
			}()
		}
	}()

	return nil
}

// handleClientRead returns a json encoded response
func handleClientRead(client string, req communication.ClientReadRequest) []byte {
	infoLogger.Printf("handling:")
	genericLogger.Printf("%s", util.StructToPrettyJsonString(req))

	storage.Lock()
	maintainer.Lock()
	defer func() {
		maintainer.Unlock()
		storage.Unlock()
	}()

	v, ok := storage.storage[req.Key]
	if !ok {
		return makeFailResp(fmt.Sprintf("key %q does not exist", req.Key))
	}

	// update dependency data
	d := maintainer.dependencyByClient[client]
	d = append(d, communication.DependencyData{
		Key:                   req.Key,
		OriginalServer:        v.originalServer,
		LamportClockTimestamp: v.lamportClockTimestamp,
	})

	resp, _ := json.Marshal(communication.ClientReadResponse{
		Op:             req.Op,
		Result:         communication.Success,
		DetailedResult: "read is successful",
		Key:            req.Key,
		Value:          v.value,
	})
	return resp
}

// handleClientWrite returns a json encoded response
func handleClientWrite(client string, req communication.ClientWriteRequest) []byte {
	infoLogger.Printf("handling:")
	genericLogger.Printf("%s", util.StructToPrettyJsonString(req))

	k := req.Key
	v := req.Value

	storage.Lock()
	maintainer.Lock()
	clock.Lock()

	clock.clock++
	storage.storage[k] = valueOfKey{
		value:                 v,
		originalServer:        selfHostPort,
		lamportClockTimestamp: clock.clock,
	}
	maintainer.dependencyByClient[client] = make([]communication.DependencyData, 0)

	resp, _ := json.Marshal(communication.ClientWriteResponse{
		Op:             req.Op,
		Result:         communication.Success,
		DetailedResult: "write is successful",
		Key:            k,
		Value:          v,
	})

	// perform replicated write
	go func() {
		defer func() {
			clock.Unlock()
			maintainer.Unlock()
			storage.Unlock()
		}()

		req, _ := json.Marshal(communication.ServerReplicatedWriteRequest{
			Op:           communication.ReplicatedWrite,
			Key:          k,
			Value:        v,
			Dependencies: maintainer.dependencyByClient[client],
			Client:       client,
			Clock:        clock.clock,
		})

		for _, hp := range otherServersHostPorts {
			dialer := net.Dialer{Timeout: 3 * time.Second}
			conn, err := dialer.Dial("tcp", hp)
			if err != nil {
				errorLogger.Printf("%v", err)
			}

			if _, err := conn.Write(req); err != nil {
				errorLogger.Printf("%v", err)
			}
			_ = conn.Close()
		}
	}()

	return resp
}

// handleClientWrite returns a json encoded response
func handleServerReplicatedWrite(req communication.ServerReplicatedWriteRequest) {
	infoLogger.Printf("handling:")
	genericLogger.Printf("%s", util.StructToPrettyJsonString(req))

	k := req.Key
	v := req.Value
	defer func() {
		clock.Lock()
		clock.clock = newLamportsClock(clock.clock, req.Clock)
		clock.Unlock()
		infoLogger.Printf("committed %q->%q", k, v)
	}()

	if len(req.Dependencies) == 0 {
		storage.Lock()
		defer storage.Unlock()
		storage.storage[k] = valueOfKey{
			value:                 v,
			originalServer:        req.OriginalServer,
			lamportClockTimestamp: req.Clock,
		}
		return
	}

	dependencies := req.Dependencies
	sort.Slice(dependencies, func(i, j int) bool {
		return dependencies[i].LamportClockTimestamp < dependencies[j].LamportClockTimestamp
	})

	storage.Lock()
	for _, dependency := range dependencies {
		for {
			if storedValue, ok := storage.storage[k]; !ok {
				storage.Unlock()
				time.Sleep(1 * time.Second)
				storage.Lock()
			} else {
				if storedValue.originalServer == dependency.OriginalServer &&
					storedValue.lamportClockTimestamp == dependency.LamportClockTimestamp {
					break
				}
				storage.Unlock()
				time.Sleep(1 * time.Second)
				storage.Lock()
			}
		}
	}

	storage.storage[k] = valueOfKey{
		value:                 v,
		originalServer:        req.OriginalServer,
		lamportClockTimestamp: req.Clock,
	}
	storage.Unlock()
}

func makeFailResp(detailedResult string) []byte {
	resp, _ := json.Marshal(struct {
		Result         communication.OperationResult
		DetailedResult string
	}{
		Result:         communication.Fail,
		DetailedResult: detailedResult,
	})

	return resp
}

func newLamportsClock(local, message uint64) uint64 {
	return uint64(math.Max(float64(local), float64(message+1)))
}
