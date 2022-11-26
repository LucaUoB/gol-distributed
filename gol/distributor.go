package gol

import (
	"fmt"
	"net"
	"net/rpc"
	"time"
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
	p                 Params
	c                 distributorChannels
	broker            *rpc.Client
	world             [][]byte
	finalStrips       [][][]byte
	commandCompletion chan int
	address           string
	cellCount         int
	killed            bool
}

var (
	dist = Distributor{
		address: "127.0.0.1:8030",
	}
	registered = false
)

func makeWorld(height, width int) [][]byte {
	world := make([][]byte, height)
	for i := range world {
		world[i] = make([]byte, width)
	}
	return world
}
func paused(keyPresses <-chan rune) {
	for {
		select {
		case k := <-keyPresses:
			if k == 'p' {
				fmt.Println("Continuing")
				return
			}
		default:
		}
	}
}
func (d *Distributor) runIO(turn int, ticker *time.Ticker) bool {
	select {
	case k := <-d.c.keyPresses:
		switch k {
		case 's':
			d.broadcastCommand(stubs.ReturnStrip)
			outputPGM(d.outputWorld(), d.c, d.p, turn)
		case 'q':
			d.broadcastCommand(stubs.ReturnStrip)
			return true
		case 'k':
			d.broadcastCommand(stubs.Kill)
			d.killed = true
			return true
		case 'p':
			paused(d.c.keyPresses)
		}
	default:
	}
	d.c.events <- TurnComplete{CompletedTurns: turn}
	select {
	case <-ticker.C:
		d.cellCount = 0
		d.broadcastCommand(stubs.CountCells)
		d.c.events <- AliveCellsCount{
			CompletedTurns: turn + 1,
			CellsCount:     d.cellCount,
		}

	default:
	}
	return false
}
func (d *Distributor) runImage(startTurn int) ([][]byte, int) {
	if startTurn == 0 {
		d.publishStrips() // Publish strips and wait for strips received flag
		d.broadcastCommand(stubs.AssignAddresses)
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	// d.runIO(1, ticker)
	for turn := startTurn; turn < d.p.Turns; turn++ {
		d.broadcastCommand(stubs.ExecuteTurn)
		if d.runIO(turn, ticker) {
			return d.outputWorld(), turn
		}
	}
	d.broadcastCommand(stubs.Finish)
	return d.outputWorld(), d.p.Turns
}
func (d *Distributor) outputWorld() [][]byte {
	finalWorld := makeWorld(0, 0)
	for _, strip := range d.finalStrips {
		finalWorld = append(finalWorld, strip...)
	}
	return finalWorld
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
	step := d.p.ImageHeight / d.p.Threads
	for i := 0; i < d.p.Threads-1; i++ {
		strip := stubs.StripContainer{
			Strip:  d.world[i*step : step*(i+1)],
			Order:  i,
			StartY: i * step,
		}
		d.broker.Call(stubs.PublishStrip, stubs.PublishStripRequest{Strip: strip}, nil)
	}
	strip := stubs.StripContainer{
		Strip:  d.world[(d.p.Threads-1)*step:],
		Order:  d.p.Threads - 1,
		StartY: (d.p.Threads - 1) * step,
	}
	d.broker.Call(stubs.PublishStrip, stubs.PublishStripRequest{Strip: strip}, nil)
	d.receiveCommandCompletion()
}
func (d *Distributor) broadcastCommand(command stubs.WorkerCommand) {
	status := new(stubs.StatusReport)
	d.broker.Call(stubs.PublishCommand, stubs.Command{WorkerCommand: command}, status)
	d.receiveCommandCompletion()
}
func (d *Distributor) receiveCommandCompletion() {
	for i := 0; i < d.p.Threads; i++ {
		<-d.commandCompletion
	}
}
func (d *Distributor) collectStrips(order int, strip [][]byte) {
	d.finalStrips[order] = strip
}
func (d *Distributor) CommandExecuted(req stubs.WorkerReport, res *stubs.StatusReport) (err error) {
	switch req.Command {
	case stubs.CountCells:
		d.cellCount += req.AliveCount
	case stubs.ReceiveStrip:
	case stubs.ExecuteTurn:
	case stubs.ReturnStrip:
		d.collectStrips(req.WorkerReturn.Order, req.WorkerReturn.Strip)
	case stubs.Finish:
		d.collectStrips(req.WorkerReturn.Order, req.WorkerReturn.Strip)
	}
	d.commandCompletion <- 1
	return
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
	brokerAddr := "127.0.0.1:8040"

	// Distributor RPC server
	if !registered {
		listener, _ := net.Listen("tcp", dist.address)
		go func() {
			rpc.Accept(listener)
		}()
	}

	// Subscribing to the broker instance
	broker, err := rpc.Dial("tcp", brokerAddr)
	if err != nil {
		fmt.Println(err)
		return
	}
	dist.p = p
	dist.c = c
	dist.finalStrips = make([][][]byte, p.Threads)
	dist.commandCompletion = make(chan int, p.Threads+1)
	dist.broker = broker
	status := new(stubs.DistributorSubscriptionResponse)
	err = broker.Call(stubs.RegisterDistributor, stubs.DistributorSubscription{
		Address: dist.address,
		Threads: p.Threads,
		Height:  p.ImageHeight,
		Width:   p.ImageWidth,
	}, &status)
	if err != nil {
		fmt.Println(err)
		return
	}
	if !registered {
		err = rpc.Register(&dist)
		registered = true
	}
	completedWorld, turn := dist.runImage(status.TurnsProcessed)
	outputPGM(completedWorld, c, p, turn)
	if !dist.killed {
		err = broker.Call(stubs.UnlockWorkers, stubs.StatusReport{}, &stubs.StatusReport{})
		if err != nil {
			fmt.Println(err)
		}
	}
	err = broker.Close()
	c.events <- FinalTurnComplete{
		CompletedTurns: turn,
		Alive:          getAllAlive(completedWorld, 0),
	}

	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- StateChange{turn, Quitting}
	close(c.events)
}
