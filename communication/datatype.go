package communication

const (
	Read  ClientServerOperation = "read"
	Write ClientServerOperation = "write"

	ReplicatedWrite ServerServerOperation = "replicated_write"

	Success OperationResult = "success"
	Fail    OperationResult = "fail"
)

type ClientServerOperation string
type ServerServerOperation string
type OperationResult string

type DependencyData struct {
	Key                   string
	OriginalServer        string
	LamportClockTimestamp uint64
}

type ClientReadRequest struct {
	Op  ClientServerOperation
	Key string
}

type ClientReadResponse struct {
	Op             ClientServerOperation
	Result         OperationResult
	DetailedResult string
	Key            string
	Value          string
}

type ClientWriteRequest struct {
	Op    ClientServerOperation
	Key   string
	Value string
}

type ClientWriteResponse struct {
	Op             ClientServerOperation
	Result         OperationResult
	DetailedResult string
	Key            string
	Value          string
}

type ServerReplicatedWriteRequest struct {
	Op             ServerServerOperation
	Key            string
	Value          string
	Dependencies   []DependencyData
	Client         string
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
