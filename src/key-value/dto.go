package key_value

type CommandExecutionInfo struct {
	Found   bool
	Value   any
	Message string
	Success bool
}

type ErrorResponse struct {
	Error string
}

type SetKeyValueRequest struct {
	Value any `json:"value"`
}

type CompareAndSetKeyValueRequest struct {
	OldValue any
	NewValue any
}

type CommandResponse struct {
	IsLeader  bool
	LeaderId  string
	RequestId string
}

type GetCommandExecutionInfoResponse struct {
	IsLeader bool
	LeaderId string
	Info     CommandExecutionInfo
}

type LeaderInfo struct {
	IsLeader bool
	LeaderId string
}
