package stubs

var CommandExecuted = "Distributor.CommandExecuted"
var ExecuteCommand = "Worker.ExecuteCommand"
var StripReceive = "Worker.StripReceive"
var AddressReceive = "Worker.AddressReceive"
var WorkerSubscribed = "Distributor.WorkerSubscribed"
var Subscribe = "Broker.Subscribe"
var PublishStrip = "Broker.PublishStrip"
var PublishCommand = "Broker.PublishCommand"
var RegisterDistributor = "Broker.RegisterDistributor"
var SendRow = "Worker.SendRow"

type AddressPair struct {
	Up   string
	Down string
}

type StripContainer struct {
	Strip [][]byte
	Order int
}

type WorkerReport struct {
	WorkerReturn    *StripContainer
	Command         WorkerCommand
	CommandExecuted bool
}

type Subscription struct {
	Address string
}
type DistributorSubscription struct {
	Address string
	Threads int
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
	Finish
	CountCells
	GetAlive
	Kill
	GetFlippedCells
	AssignAddresses
	ReceiveStrip
)
