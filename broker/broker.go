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
	strips   chan stubs.StripContainer
	commands chan stubs.WorkerCommand
}

var (
	brokerChannels = BrokerChannels{
		strips:   make(chan stubs.StripContainer),
		commands: make(chan stubs.WorkerCommand),
	}
	workerAddresses   = make([]string, 16)
	addressesMX       sync.Mutex
	distributorClient *rpc.Client
	threads           int
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
// Addresses above and below the current subscribers addresses are given to that subscriber - lock mutex during access to address arr
// Subscribers await further commands from distributor

func subscriberLoop(brokerChannels BrokerChannels, client *rpc.Client, address string) {
	var order int
	for {
		select {
		case strip := <-brokerChannels.strips:
			workerResponse := new(stubs.WorkerReport)
			distributorResponse := new(stubs.StatusReport)
			err := client.Call(stubs.StripReceive, strip, workerResponse)
			if handleError(err) {
				return
			}
			order = strip.Order
			addressesMX.Lock()
			workerAddresses[order] = address
			addressesMX.Unlock()

			err = distributorClient.Call(stubs.CommandExecuted, workerResponse, distributorResponse)
			if handleError(err) {
				return
			}
		case command := <-brokerChannels.commands:
			if command == stubs.AssignAddresses {
				workerResponse := new(stubs.WorkerReport)
				addressesMX.Lock()
				addresses := stubs.AddressPair{
					Up:   workerAddresses[gol.Mod(order-1, threads)],
					Down: workerAddresses[gol.Mod(order+1, threads)],
				}
				addressesMX.Unlock()

				err := client.Call(stubs.AddressReceive, addresses, &workerResponse)
				if handleError(err) {
					return
				}

				err = distributorClient.Call(stubs.CommandExecuted, workerResponse, nil)
				if handleError(err) {
					return
				}
				fmt.Println("Addresses Assigned")
			} else {
				workerResponse := new(stubs.WorkerReport)
				distributorResponse := new(stubs.StatusReport)
				err := client.Call(stubs.ExecuteCommand, stubs.Command{WorkerCommand: command}, &workerResponse)
				if handleError(err) {
					return
				}
				err = distributorClient.Call(stubs.CommandExecuted, workerResponse, &distributorResponse)
				if handleError(err) {
					return
				}
			}
		}
	}
}

func subscribe(workerAddress string) (err error) {
	fmt.Println("Subscription request")
	client, err := rpc.Dial("tcp", workerAddress)
	if err == nil {
		go subscriberLoop(brokerChannels, client, workerAddress)
	} else {
		fmt.Println("Error subscribing ", workerAddress)
		fmt.Println(err)
		return err
	}
	fmt.Println("Subscribed")
	// tell distributor that a worker has subscribed
	err = distributorClient.Call(stubs.WorkerSubscribed, stubs.WorkerReport{}, &stubs.StatusReport{})
	if err != nil {
		fmt.Println(err)
		return err
	}
	return
}

type Broker struct{}

func (b *Broker) Subscribe(req stubs.Subscription, res *stubs.StatusReport) (err error) {
	err = subscribe(req.Address)
	if err != nil {
		res.Message = "Error during subscription"
	}
	return err
}
func (b *Broker) PublishStrip(req stubs.PublishStripRequest, res *stubs.StatusReport) (err error) {
	fmt.Println("Publishing Strips")
	brokerChannels.strips <- req.Strip
	return error(nil)
}
func (b *Broker) PublishCommand(req stubs.Command, res *stubs.StatusReport) (err error) {
	brokerChannels.commands <- req.WorkerCommand
	return error(nil)
}
func (b *Broker) RegisterDistributor(req stubs.DistributorSubscription, res *stubs.StatusReport) (err error) {
	distributorClient, err = rpc.Dial("tcp", req.Address)
	threads = req.Threads
	if err != nil {
		res.Message = "Error during distributor registration"
	}
	return err
}

func main() {
	pAddr := flag.String("port", "8040", "Port to listen on")
	flag.Parse()
	rpc.Register(&Broker{})
	listenser, _ := net.Listen("tcp", ":"+*pAddr)
	defer listenser.Close()
	rpc.Accept(listenser)
}
