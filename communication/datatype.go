package communication

const (
	Connect = "connect"
	Read    = "read"
	Write   = "write"

	ReplicatedWrite = "replicated_write"

	Success OperationResult = "success"
	Fail    OperationResult = "fail"
)

type OperationResult string

type DependencyData struct {
	Key                   string
	OriginalServer        string
	LamportClockTimestamp uint64
}

type ClientConnectRequest struct {
	Op   string
	Args ClientConnectRequestArgs
}

type ClientConnectRequestArgs struct {
	ClientId string
}

type ClientConnectResponse struct {
	Op             string
	Result         OperationResult
	DetailedResult string
}

type ClientReadRequest struct {
	Op   string
	Args ClientReadRequestArgs
}

type ClientReadRequestArgs struct {
	ClientId string
	Key      string
}

type ClientReadResponse struct {
	Op             string
	Result         OperationResult
	DetailedResult string
	Key            string
	Value          string
}

type ClientWriteRequest struct {
	Op   string
	Args ClientWriteRequestArgs
}

type ClientWriteRequestArgs struct {
	ClientId string
	Key      string
	Value    string
}

type ClientWriteResponse struct {
	Op             string
	Result         OperationResult
	DetailedResult string
	Key            string
	Value          string
}

type ServerReplicatedWriteRequest struct {
	Op   string
	Args ServerReplicatedWriteRequestArgs
}

type ServerReplicatedWriteRequestArgs struct {
	Key            string
	Value          string
	ClientId       string
	Dependencies   []DependencyData
	OriginalServer string
	Clock          uint64
}

//type ServerReplicatedWriteResponse struct {
//	Op             ServerServerOperation
//	Result         OperationResult
//	DetailedResult string
//	Key            string
//	Value          string
//}
