package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/stubs"
)

type BrokerChannels struct {
	strips        chan stubs.StripContainer
	commands      chan stubs.WorkerCommand
	workerLocks   []chan int
	workerUnlocks []chan int
}
type WorkerChannelContainer struct {
	strips   chan stubs.StripContainer
	commands chan stubs.WorkerCommand
	lock     chan WorkerLock
	kill     chan int
}

type Broker struct {
	workerChannelsArr []*WorkerChannelContainer
	distributorClient *rpc.Client
	workerAddresses   []string
	addressesMX       sync.Mutex
	workersRequired   int
	workersSubscribed int
	turnsProcessed    int
	stripsReceived    int
	subscribed        chan int
	done              chan int
	height, width     int
	working           bool
}
type WorkerLock uint8

const (
	Lock WorkerLock = iota
	Unlock
)

var (
	brokerChannels = BrokerChannels{
		strips:   make(chan stubs.StripContainer),
		commands: make(chan stubs.WorkerCommand),
	}
)

func handleError(err error) bool {
	if err != nil {
		fmt.Println("Error")
		fmt.Println(err)
		fmt.Println("Closing subscriber thread")
	}
	return err != nil
}

// Subscriber flow:
// Subscribe - starts subscriber loop waiting for a new strip from the distributor
// Once all strips have been received and the distributor has received <threads> responses it sends the AssignAddresses command
// Addresses above and below the current subscribers address are given to that subscriber - lock mutex during access to address arr
// Subscribers await further commands from distributor

func (b *Broker) workerLocked(workerChannels WorkerChannelContainer, client *rpc.Client) {
	for {
		select {
		case <-workerChannels.lock:
			return
		case <-workerChannels.kill:
			// Execute Kill code
			workerResponse := new(stubs.WorkerReport)
			err := client.Call(stubs.ExecuteCommand, stubs.Command{WorkerCommand: stubs.Kill}, &workerResponse)
			handleError(err)
			b.processKillCommand(client)
		}
	}
}

// TODO zero strips received, check distributor publish commands events
func (b *Broker) subscriberLoop(client *rpc.Client, address string, workerChannels WorkerChannelContainer) {
	var order int
	for {
		select {
		case <-workerChannels.lock:
			b.workerLocked(workerChannels, client)
		case strip := <-workerChannels.strips:
			workerResponse := new(stubs.WorkerReport)
			distributorResponse := new(stubs.StatusReport)
			err := client.Call(stubs.StripReceive, strip, workerResponse)
			if handleError(err) {
				return
			}
			order = strip.Order
			b.addressesMX.Lock()
			b.workerAddresses[order] = address
			b.addressesMX.Unlock()

			err = b.distributorClient.Call(stubs.CommandExecuted, workerResponse, distributorResponse)
			if handleError(err) {
				return
			}
		case command := <-workerChannels.commands: // Figure out a way to stop reading the channel if not working
			if command == stubs.AssignAddresses {
				workerResponse := new(stubs.WorkerReport)
				b.addressesMX.Lock()
				addresses := stubs.AddressPair{
					Up:   b.workerAddresses[gol.Mod(order-1, b.workersRequired)],
					Down: b.workerAddresses[gol.Mod(order+1, b.workersRequired)],
				}
				b.addressesMX.Unlock()

				err := client.Call(stubs.AddressReceive, addresses, &workerResponse)
				if handleError(err) {
					return
				}

				err = b.distributorClient.Call(stubs.CommandExecuted, workerResponse, nil)
				if handleError(err) {
					return
				}
			} else {
				b.executeCommand(command, client)
			}
		}
	}
}
func (b *Broker) processKillCommand(client *rpc.Client) {
	b.workersSubscribed--
	err := client.Close()
	handleError(err)
	// If there are no workers left to kill, shut down broker
	if b.workersSubscribed == 0 {
		b.done <- 1
	}
}

// Redesign this system to have individual command channels
func (b *Broker) executeCommand(command stubs.WorkerCommand, client *rpc.Client) {
	workerResponse := new(stubs.WorkerReport)
	distributorResponse := new(stubs.StatusReport)
	err := client.Call(stubs.ExecuteCommand, stubs.Command{WorkerCommand: command}, &workerResponse)
	if handleError(err) {
		return
	}
	err = b.distributorClient.Call(stubs.CommandExecuted, workerResponse, &distributorResponse)
	if handleError(err) {
		return
	}
	if command == stubs.Kill {
		b.processKillCommand(client)
	} else if command == stubs.Finish {
		b.turnsProcessed = 0
	}
}
func (b *Broker) workersReady() {
	for b.workersSubscribed < b.workersRequired {
		// blocks until it receives enough subscriptions from workers
		<-b.subscribed
	}
	for i := b.workersRequired; i < b.workersSubscribed; i++ {
		b.workerChannelsArr[i].lock <- 1
	}
	b.working = true
}
func (b *Broker) subscribe(workerAddress string) (err error) {
	fmt.Println("Subscription request")
	client, err := rpc.Dial("tcp", workerAddress)
	if handleError(err) {
		return
	}

	w := WorkerChannelContainer{
		strips:   make(chan stubs.StripContainer),
		commands: make(chan stubs.WorkerCommand, 2),
		lock:     make(chan WorkerLock),
		kill:     make(chan int),
	}
	b.workerChannelsArr = append(b.workerChannelsArr, &w)
	if err == nil && !b.working {
		go b.subscriberLoop(client, workerAddress, w)
	} else {
		fmt.Println("Error subscribing ", workerAddress)
		fmt.Println(err)
		return err
	}
	fmt.Println("Subscribed")
	b.workersSubscribed++
	b.subscribed <- 1
	return
}

func (b *Broker) Subscribe(req stubs.Subscription, res *stubs.StatusReport) (err error) {
	err = b.subscribe(req.Address)
	if err != nil {
		res.Message = "Error during subscription"
	}
	return err
}
func (b *Broker) UnlockWorkers(req stubs.StatusReport, res *stubs.StatusReport) (err error) {
	for i := b.workersRequired; i < b.workersSubscribed; i++ {
		b.workerChannelsArr[i].lock <- 1
	}
	b.working = false
	return
}
func (b *Broker) PublishStrip(req stubs.PublishStripRequest, res *stubs.StatusReport) (err error) {
	// Lock workers if there are too many
	fmt.Println("Publishing Strips")
	b.workerChannelsArr[b.stripsReceived].strips <- req.Strip
	b.stripsReceived++
	return
}
func (b *Broker) PublishCommand(req stubs.Command, res *stubs.StatusReport) (err error) {
	for i := 0; i < b.workersRequired; i++ {
		b.workerChannelsArr[i].commands <- req.WorkerCommand
	}
	if req.WorkerCommand == stubs.ExecuteTurn {
		b.turnsProcessed++
	} else if req.WorkerCommand == stubs.Kill {
		for i := b.workersRequired; i < b.workersSubscribed; i++ {
			b.workerChannelsArr[i].kill <- 1
		}
	}
	return
}
func (b *Broker) RegisterDistributor(req stubs.DistributorSubscription, res *stubs.DistributorSubscriptionResponse) (err error) {
	b.distributorClient, err = rpc.Dial("tcp", req.Address)
	if err != nil {
		fmt.Println(err)
	}
	// If new distributor params are the same then continue processing the same image
	if b.workersRequired == req.Threads && b.height == req.Height && b.width == req.Width {
		res.TurnsProcessed = b.turnsProcessed
	} else {
		res.TurnsProcessed = 0
		b.height = req.Height
		b.width = req.Width
	}
	b.stripsReceived = 0
	b.workersRequired = req.Threads
	b.workersReady()
	return err
}
func (b *Broker) DeregisterDistributor(req stubs.DistributorSubscription, res *stubs.StatusReport) (err error) {
	b.distributorClient = nil
	// b.distributorClient.Close()
	return
}

func main() {
	pAddr := flag.String("port", "8040", "Port to listen on")
	flag.Parse()
	b := Broker{
		distributorClient: nil,
		workerAddresses:   make([]string, 16),
		addressesMX:       sync.Mutex{},
		workersRequired:   0,
		workersSubscribed: 0,
		subscribed:        make(chan int, 16),
		done:              make(chan int),
		working:           false,
	}
	err := rpc.Register(&b)
	handleError(err)
	listenser, _ := net.Listen("tcp", ":"+*pAddr)
	go func() {
		defer listenser.Close()
		rpc.Accept(listenser)
	}()
	<-b.done
}
