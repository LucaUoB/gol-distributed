package stubs

var ExecuteCommand = "Worker.ExecuteCommand"
var StripReceive = "Worker.StripReceive"
var AddressReceive = "Worker.AddressReceive"
var Subscribe = "Broker.Subscribe"
var PublishStrip = "Broker.PublishStrip"
var PublishCommand = "Broker.PublishCommand"
var RegisterDistributor = "Broker.RegisterDistributor"
var SendRow = "Worker.SendRow"
var UnlockWorkers = "Broker.UnlockWorkers"

type AddressPair struct {
	Up   string
	Down string
}
type RowContainer struct {
	Row  []int
	Type int
}
type StripContainer struct {
	Strip  [][]byte
	Order  int
	StartY int
}
type WorkerReportArr struct {
	Arr []WorkerReport
}
type WorkerReport struct {
	WorkerReturn    *StripContainer
	Command         WorkerCommand
	CommandExecuted bool
	AliveCount      int
}

type Subscription struct {
	Address string
}
type DistributorSubscription struct {
	Address       string
	Threads       int
	Height, Width int
}
type DistributorSubscriptionResponse struct {
	TurnsProcessed int
}

type PublishStripRequest struct {
	Strip StripContainer
}

type StatusReport struct {
	Message string
}

type Command struct {
	WorkerCommand WorkerCommand
}

type WorkerCommand uint8

const (
	ExecuteTurn WorkerCommand = iota
	WorkerLock
	WorkerUnlock
	Finish
	ReturnStrip
	CountCells
	GetAlive
	Kill
	GetFlippedCells
	AssignAddresses
	ReceiveStrip
)
