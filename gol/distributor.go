package gol

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"uk.ac.bris.cs/gameoflife/stubs"
	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
	keyPresses <-chan rune
}

type Distributor struct {
	threadsSubscribed int
	p                 Params
	c                 distributorChannels
	broker            *rpc.Client
	world             [][]byte
	finalStrips       [][][]byte
	commandCompletion chan int
	worldReturn       chan stubs.StripContainer
}

func makeWorld(height, width int) [][]byte {
	world := make([][]byte, height)
	for i := range world {
		world[i] = make([]byte, width)
	}
	return world
}
func (d *Distributor) runImage() {
	d.publishStrips() // Publish strips and wait for strips received flag
	d.broadcastCommand(stubs.AssignAddresses)
	for turn := 0; turn < d.p.Turns; turn++ {
		d.broadcastCommand(stubs.ExecuteTurn)
		fmt.Println(turn)
	}
	d.broadcastCommand(stubs.Finish)
	finalWorld := makeWorld(0, 0)
	for _, strip := range d.finalStrips {
		finalWorld = append(finalWorld, strip...)
	}
	d.worldReturn <- stubs.StripContainer{
		Strip: finalWorld,
		Order: 0,
	}

}
func (d *Distributor) readImage() {
	world := makeWorld(d.p.ImageHeight, d.p.ImageWidth)
	d.c.ioCommand <- ioInput
	d.c.ioFilename <- fmt.Sprintf("%dx%d", d.p.ImageHeight, d.p.ImageWidth)

	for row := 0; row < d.p.ImageHeight; row++ {
		for col := 0; col < d.p.ImageWidth; col++ {
			world[row][col] = <-d.c.ioInput
		}
	}
	d.world = world
}
func (d *Distributor) publishStrips() {
	d.readImage()
	step := d.p.ImageHeight / d.threadsSubscribed
	for i := 0; i < d.threadsSubscribed-1; i++ {
		strip := stubs.StripContainer{
			Strip: d.world[i*step : step*(i+1)],
			Order: i,
		}
		d.broker.Call(stubs.PublishStrip, stubs.PublishStripRequest{Strip: strip}, nil)
	}
	strip := stubs.StripContainer{
		Strip: d.world[(d.threadsSubscribed-1)*step:],
		Order: d.threadsSubscribed - 1,
	}
	d.broker.Call(stubs.PublishStrip, stubs.PublishStripRequest{Strip: strip}, nil)
	d.receiveCommandCompletion()
}
func (d *Distributor) broadcastCommand(command stubs.WorkerCommand) {
	for i := 0; i < d.threadsSubscribed; i++ {
		status := new(stubs.StatusReport)
		d.broker.Call(stubs.PublishCommand, stubs.Command{WorkerCommand: command}, status)
	}
	d.receiveCommandCompletion()
}
func (d *Distributor) receiveCommandCompletion() {
	for i := 0; i < d.threadsSubscribed; i++ {
		<-d.commandCompletion
	}
}

func (d *Distributor) WorkerSubscribed(req stubs.WorkerReport, res *stubs.StatusReport) (err error) {
	// check for p.threads workers
	d.threadsSubscribed++
	fmt.Println("Worker Subscribed")
	if d.threadsSubscribed == d.p.Threads {
		// wait here until world is not nil (read from file)
		go d.runImage()
	}
	return error(nil)
}
func (d *Distributor) CommandExecuted(req stubs.WorkerReport, res *stubs.StatusReport) (err error) {
	switch req.Command {
	case stubs.ReceiveStrip:
	case stubs.ExecuteTurn:
		// fmt.Println("Command Executed: Execute Turn")
	case stubs.Finish:
		fmt.Println("Command Executed: Finish")
		d.finalStrips[req.WorkerReturn.Order] = req.WorkerReturn.Strip
	}
	d.commandCompletion <- 1
	return error(nil)
}
func getAllAlive(world [][]byte, startY int) []util.Cell {
	var aliveCells []util.Cell
	for i := range world {
		for f := range world[i] {
			if world[i][f] == 255 {
				aliveCells = append(aliveCells, util.Cell{X: f, Y: i + startY})
			}
		}
	}
	return aliveCells
}
func outputPGM(world [][]byte, c distributorChannels, p Params, turn int) {
	c.ioCommand <- ioOutput
	c.ioFilename <- fmt.Sprintf("%dx%dx%d", p.ImageHeight, p.ImageWidth, turn)
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.ioOutput <- world[y][x]
		}
	}
}

// distributorClient divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels) {
	pAddr := flag.String("port", "127.0.0.1:8030", "Port to listen on")
	brokerAddr := flag.String("broker", "127.0.0.1:8040", "Address of broker instance")
	flag.Parse()

	// Distributor RPC server
	listener, _ := net.Listen("tcp", *pAddr)
	go func() {
		defer listener.Close()
		rpc.Accept(listener)
	}()

	// Subscribing to the broker instance
	client, err := rpc.Dial("tcp", *brokerAddr)
	if err != nil {
		fmt.Println(err)
		return
	}

	status := new(stubs.StatusReport)
	err = client.Call(stubs.RegisterDistributor, stubs.DistributorSubscription{
		Address: *pAddr,
		Threads: p.Threads,
	}, &status)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()
	d := Distributor{
		threadsSubscribed: 0,
		p:                 p,
		c:                 c,
		broker:            client,
		world:             nil,
		commandCompletion: make(chan int, p.Threads),
		finalStrips:       make([][][]byte, p.Threads),
		worldReturn:       make(chan stubs.StripContainer),
	}
	err = rpc.Register(&d)

	completedWorld := <-d.worldReturn
	outputPGM(completedWorld.Strip, c, p, p.Turns)
	c.events <- FinalTurnComplete{
		CompletedTurns: p.Turns,
		Alive:          getAllAlive(completedWorld.Strip, 0),
	}

	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- StateChange{p.Turns, Quitting}
	close(c.events)
}
