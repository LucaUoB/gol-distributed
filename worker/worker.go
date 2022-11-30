package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"time"
	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/stubs"
	"uk.ac.bris.cs/gameoflife/util"
)

func calculateNextStep(strip [][]byte) [][]byte {
	newStrip := make([][]byte, len(strip))
	for i := range newStrip {
		newStrip[i] = make([]byte, len(strip[i]))
	}
	for i := 0; i < len(strip)-1; i++ {
		for f := 0; f < len(strip[i]); f++ {
			newStrip[i][f] = 0
			count := getNeighborCount(util.Cell{X: f, Y: i}, strip)
			if count == 3 || (count == 2 && strip[i][f] > 0) {
				newStrip[i][f] = 255
			}
		}
	}
	// Remove top and bottom rows
	return newStrip[1 : len(newStrip)-1]
}

func getNeighborCount(c util.Cell, world [][]byte) int {
	count := 0
	for i := -1; i <= 1; i++ {
		for f := -1; f <= 1; f++ {
			y := c.Y + i
			if y < 0 {
				y = 0
			} else if y >= len(world) {
				y = len(world) - 1
			}
			x := gol.Mod(c.X+f, len(world[y]))
			if x != c.X || y != c.Y {
				if world[y][x] != 0 {
					count++
				}
			}
		}
	}
	return count
}

type Worker struct {
	commonChannels   commonChannels
	localWorkers     []*localWorker
	commands         chan stubs.WorkerCommand
	strip            *stubs.StripContainer
	rows             chan stubs.RowContainer
	up, down, broker *rpc.Client
	commandResponse  chan stubs.WorkerReport
	done             chan int
	threads          int
	dp               bool
}
type localWorkerCommand uint8

const (
	MaxWidth  = 5120
	MaxHeight = 5120
	Threads   = 2
)

const (
	executeTurn localWorkerCommand = iota
	finish
	countCells
	reset
	getAlive
	kill
	getFlippedCells
)

type commonChannels struct {
	aliveCount       chan int
	workerReturns    chan stubs.StripContainer
	commandCompleted chan int
	aliveCells       chan util.Cell
	flippedCells     chan util.Cell
}
type workerChannels struct {
	workerReturn chan stubs.StripContainer
	worldStripIn chan byte
}
type localWorkerChannels struct {
	inUp          chan byte
	inDown        chan byte
	outUp         chan byte
	outDown       chan byte
	workerCommand chan localWorkerCommand
	receiveStrip  chan stubs.StripContainer
}
type localWorker struct {
	height, width, order, startY int
	worldStrip                   [][]byte
	localWorkerChannels          *localWorkerChannels
	commonChannels               *commonChannels
}

func getAllAlive(strip [][]byte, startY int) []util.Cell {
	var aliveCells []util.Cell
	for i := range strip {
		for f := range strip[i] {
			if strip[i][f] == 255 {
				aliveCells = append(aliveCells, util.Cell{X: f, Y: i + startY})
			}
		}
	}
	return aliveCells
}
func (w *Worker) receiveRow() {
	row := <-w.rows
	newRow := make([]byte, len(w.strip.Strip[0]))
	for _, p := range row.Row {
		newRow[p] = 255
	}
	if row.Type == 0 {
		w.strip.Strip = append(w.strip.Strip, newRow)
	} else {
		w.strip.Strip = append([][]byte{newRow}, w.strip.Strip...)
	}
}
func (w *Worker) startLocalWorkers(localWorkerChannels []*localWorkerChannels, commonChannels *commonChannels) {
	w.commonChannels = *commonChannels
	for i := 0; i < w.threads; i++ {
		worker := localWorker{
			localWorkerChannels: localWorkerChannels[i],
			commonChannels:      commonChannels,
		}
		w.localWorkers = append(w.localWorkers, &worker)
		go worker.localWorkerLoop()
	}
}
func (w *Worker) createLocalWorkerChannels() ([]*localWorkerChannels, commonChannels) {
	var workerChannelsArr []*localWorkerChannels
	commonChans := commonChannels{
		aliveCount:       make(chan int, w.threads),
		workerReturns:    make(chan stubs.StripContainer, w.threads),
		commandCompleted: make(chan int, w.threads),
		aliveCells:       make(chan util.Cell, MaxWidth*MaxHeight),
		flippedCells:     make(chan util.Cell, MaxWidth*MaxHeight),
	}
	workerChannelsArr = append(workerChannelsArr, &localWorkerChannels{
		inUp:          make(chan byte, MaxWidth),
		inDown:        make(chan byte, MaxWidth),
		outUp:         make(chan byte, MaxWidth),
		outDown:       make(chan byte, MaxWidth),
		workerCommand: make(chan localWorkerCommand, w.threads),
		receiveStrip:  make(chan stubs.StripContainer),
	})
	for i := 1; i < w.threads; i++ {
		workerChannelsArr = append(workerChannelsArr, &localWorkerChannels{
			inUp:          make(chan byte, MaxWidth),
			inDown:        nil,
			outUp:         make(chan byte, MaxWidth),
			outDown:       nil,
			workerCommand: make(chan localWorkerCommand, w.threads),
			receiveStrip:  make(chan stubs.StripContainer),
		})
		workerChannelsArr[i-1].outDown = workerChannelsArr[i].inUp
		workerChannelsArr[i-1].inDown = workerChannelsArr[i].outUp
	}
	workerChannelsArr[len(workerChannelsArr)-1].outDown = workerChannelsArr[0].inUp
	workerChannelsArr[len(workerChannelsArr)-1].inDown = workerChannelsArr[0].outUp
	return workerChannelsArr, commonChans
}
func (w *Worker) broadcastCommand(command localWorkerCommand) {
	for i := 0; i < len(w.localWorkers); i++ {
		w.localWorkers[i].localWorkerChannels.workerCommand <- command
	}
}
func (w *Worker) receiveCommand(c chan int) {
	for i := 0; i < len(w.localWorkers); i++ {
		<-c
	}
}
func (w *Worker) sendRow(row []byte, order int) {
	alive := make([]int, 0)
	for i, b := range row {
		if b != 0 {
			alive = append(alive, i)
		}
	}
	response := new(stubs.StatusReport)
	if order == 0 {
		w.up.Call(stubs.SendRow, stubs.RowContainer{
			Row:  alive,
			Type: order,
		}, &response)
	} else {
		w.down.Call(stubs.SendRow, stubs.RowContainer{
			Row:  alive,
			Type: order,
		}, &response)
	}
}
func (w *Worker) workerDistributorLoop() {
	for {
		select {
		case command := <-w.commands:
			switch command {
			case stubs.Kill:
				w.returnStrip(stubs.Finish)
				time.Sleep(2 * time.Second)
				w.done <- 1

			case stubs.CountCells:
				count := 0
				w.broadcastCommand(countCells)
				for i := 0; i < len(w.localWorkers); i++ {
					count += <-w.commonChannels.aliveCount
				}
				w.commandResponse <- stubs.WorkerReport{
					WorkerReturn:    nil,
					Command:         stubs.CountCells,
					CommandExecuted: true,
					AliveCount:      count,
				}
			case stubs.ExecuteTurn:
				// Send top and bottom rows
				w.sendRow(w.strip.Strip[0], 0)
				w.sendRow(w.strip.Strip[len(w.strip.Strip)-1], 1)
				// Receive top and bottom rows
				w.receiveRow()
				w.receiveRow()
				// Broadcast command to local workers to calculate next step
				w.broadcastCommand(executeTurn)
				w.receiveCommand(w.commonChannels.commandCompleted)
				w.commandResponse <- stubs.WorkerReport{
					WorkerReturn:    nil,
					CommandExecuted: true,
					Command:         command,
				}
			case stubs.ReturnStrip:
				w.dpReturnStrip(command)
			case stubs.Finish:
				w.dpReturnStrip(command)
			}
		}
	}
}
func makeWorld(height, width int) [][]byte {
	world := make([][]byte, height)
	for i := range world {
		world[i] = make([]byte, width)
	}
	return world
}
func (lw *localWorker) reset() {
	lw.worldStrip = makeWorld(lw.height, lw.width)
}
func channelArrWrite(channel chan byte, arr []byte) {
	for i := 0; i < len(arr); i++ {
		channel <- arr[i]
	}
}
func channelArrRead(channel chan byte, l int) []byte {
	arr := make([]byte, l)
	for i := 0; i < l; i++ {
		arr[i] = <-channel
	}
	return arr
}
func (lw *localWorker) localWorkerLoop() {
	lw.reset()
	for {
		select {
		case strip := <-lw.localWorkerChannels.receiveStrip:
			lw.worldStrip = strip.Strip
			lw.startY = strip.StartY
			lw.order = strip.Order
			lw.height = len(strip.Strip)
			lw.width = len(strip.Strip[0])
		case c := <-lw.localWorkerChannels.workerCommand:
			switch c {
			case finish:
				lw.commonChannels.workerReturns <- stubs.StripContainer{
					Strip:  lw.worldStrip,
					Order:  lw.order,
					StartY: lw.startY,
				}
			case executeTurn:
				channelArrWrite(lw.localWorkerChannels.outUp, lw.worldStrip[0])
				channelArrWrite(lw.localWorkerChannels.outDown, lw.worldStrip[lw.height-1])
				lw.worldStrip = append([][]byte{channelArrRead(lw.localWorkerChannels.inUp, lw.width)}, lw.worldStrip...)
				lw.worldStrip = append(lw.worldStrip, channelArrRead(lw.localWorkerChannels.inDown, lw.width))
				lw.worldStrip = calculateNextStep(lw.worldStrip)
				lw.commonChannels.commandCompleted <- 1
			case countCells:
				lw.commonChannels.aliveCount <- len(getAllAlive(lw.worldStrip, lw.startY))
			case reset:
				lw.reset()
			}

		}
	}
}
func (w *Worker) workerLoop() {
	for {
		select {
		case command := <-w.commands:
			switch command {
			case stubs.Kill:
				w.returnStrip(stubs.Finish)
				time.Sleep(2 * time.Second)
				w.done <- 1

			case stubs.CountCells:
				w.commandResponse <- stubs.WorkerReport{
					WorkerReturn:    nil,
					Command:         stubs.CountCells,
					CommandExecuted: true,
					AliveCount:      len(getAllAlive(w.strip.Strip, w.strip.StartY)),
				}
			case stubs.ExecuteTurn:
				// Send top and bottom rows
				w.sendRow(w.strip.Strip[0], 0)
				w.sendRow(w.strip.Strip[len(w.strip.Strip)-1], 1)
				// Receive top and bottom rows
				w.receiveRow()
				w.receiveRow()
				w.strip.Strip = calculateNextStep(w.strip.Strip)
				w.commandResponse <- stubs.WorkerReport{
					WorkerReturn:    nil,
					CommandExecuted: true,
					Command:         command,
				}
			case stubs.ReturnStrip:
				w.returnStrip(command)
			case stubs.Finish:
				w.returnStrip(command)
			}
		}
	}
}
func (w *Worker) getFullWorld() {
	w.broadcastCommand(finish)
	threads := len(w.localWorkers)
	strips := make([][][]byte, threads)
	for i := 0; i < threads; i++ {
		returnedStrip := <-w.commonChannels.workerReturns
		strips[returnedStrip.Order] = returnedStrip.Strip
	}
	finalWorld := makeWorld(0, 0)
	for _, strip := range strips {
		finalWorld = append(finalWorld, strip...)
	}
	w.strip.Strip = finalWorld
}
func (w *Worker) dpReturnStrip(command stubs.WorkerCommand) {
	w.getFullWorld()
	w.commandResponse <- stubs.WorkerReport{
		WorkerReturn: &stubs.AliveCellsContainer{
			Strip:  getAllAlive(w.strip.Strip, 0),
			Order:  w.strip.Order,
			StartY: w.strip.StartY,
			Height: len(w.strip.Strip),
			Width:  len(w.strip.Strip[0]),
		},
		Command:         command,
		CommandExecuted: true,
	}
}
func (w *Worker) returnStrip(command stubs.WorkerCommand) {
	w.commandResponse <- stubs.WorkerReport{
		WorkerReturn: &stubs.AliveCellsContainer{
			Strip:  getAllAlive(w.strip.Strip, 0),
			Order:  w.strip.Order,
			StartY: w.strip.StartY,
			Height: len(w.strip.Strip),
			Width:  len(w.strip.Strip[0]),
		},
		CommandExecuted: true,
		Command:         command,
	}
}
func (w *Worker) ExecuteCommand(req stubs.Command, res *stubs.WorkerReport) (err error) {
	w.commands <- req.WorkerCommand
	r := <-w.commandResponse
	res.CommandExecuted = r.CommandExecuted
	res.WorkerReturn = r.WorkerReturn
	res.Command = r.Command
	res.AliveCount = r.AliveCount
	return
}
func (w *Worker) SendRow(req stubs.RowContainer, res *stubs.StatusReport) (err error) {
	w.rows <- req
	res.Message = "Row received"
	return
}
func (w *Worker) StripReceive(req stubs.AliveCellsContainer, res *stubs.WorkerReport) (err error) {

	strip := makeWorld(req.Height, req.Width)
	for _, cell := range req.Strip {
		strip[cell.Y][cell.X] = 255
	}
	stripContainer := stubs.StripContainer{
		Strip:  strip,
		Order:  req.Order,
		StartY: req.StartY,
	}
	w.strip = &stripContainer
	if w.dp {
		step := len(stripContainer.Strip) / w.threads
		for i := 0; i < len(w.localWorkers)-1; i++ {
			w.localWorkers[i].localWorkerChannels.receiveStrip <- stubs.StripContainer{
				Strip:  stripContainer.Strip[i*step : (i+1)*step],
				Order:  i,
				StartY: i * step,
			}
		}
		l := len(w.localWorkers) - 1
		w.localWorkers[l].localWorkerChannels.receiveStrip <- stubs.StripContainer{
			Strip:  stripContainer.Strip[l*step:],
			Order:  l,
			StartY: l * step,
		}
	}
	res.Command = stubs.ReceiveStrip
	return
}
func (w *Worker) AddressReceive(req stubs.AddressPair, res *stubs.WorkerReport) (err error) {
	w.up, _ = rpc.Dial("tcp", req.Up)
	w.down, _ = rpc.Dial("tcp", req.Down)
	res.Command = stubs.AssignAddresses
	return
}

func main() {
	pAddr := flag.String("ip", "127.0.0.1:8050", "IP and port to listen on")
	brokerAddr := flag.String("broker", "127.0.0.1:8040", "Address of broker instance")
	distributedParallel := flag.String("dp", "0", "0 for distributed, 1 for distributed parallel")
	flag.Parse()
	w := Worker{
		localWorkers:    make([]*localWorker, 0),
		commands:        make(chan stubs.WorkerCommand),
		strip:           nil,
		rows:            make(chan stubs.RowContainer, 2),
		up:              nil,
		down:            nil,
		commandResponse: make(chan stubs.WorkerReport, 2),
		broker:          nil,
		done:            make(chan int),
		threads:         Threads,
		dp:              false,
	}
	if *distributedParallel == "1" {
		w.dp = true
		localWorkerChans, commonChans := w.createLocalWorkerChannels()
		w.startLocalWorkers(localWorkerChans, &commonChans)
	}

	err := rpc.Register(&w)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Set up RPC server
	listener, err := net.Listen("tcp", *pAddr)
	if err != nil {
		fmt.Println(err)
		return
	}
	go func() {
		defer listener.Close()
		rpc.Accept(listener)
	}()

	// Subscribe to broker instance
	w.broker, _ = rpc.Dial("tcp", *brokerAddr)
	defer w.broker.Close()
	subscription := stubs.Subscription{Address: *pAddr}
	status := new(stubs.StatusReport)
	w.broker.Call(stubs.Subscribe, subscription, &status)
	fmt.Println(status.Message)
	if *distributedParallel == "1" {
		go w.workerDistributorLoop()
	} else {
		go w.workerLoop()
	}
	<-w.done
}
