package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
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
	commands         chan stubs.WorkerCommand
	strip            *stubs.StripContainer
	rows             chan stubs.StripContainer
	up, down, broker *rpc.Client
	commandResponse  chan stubs.WorkerReport
}

func (w *Worker) receiveRow() {
	row := <-w.rows
	if row.Order == 0 {
		w.strip.Strip = append(w.strip.Strip, row.Strip...)
	} else {
		w.strip.Strip = append(row.Strip, w.strip.Strip...)
	}
}
func (w *Worker) workerLoop() {
	turn := 0
	for {
		select {
		case command := <-w.commands:
			switch command {
			case stubs.ExecuteTurn:
				// Send top and bottom rows
				turn++
				response := new(stubs.StatusReport)
				err := w.up.Call(stubs.SendRow, stubs.StripContainer{
					Strip: [][]byte{w.strip.Strip[0]},
					Order: 0,
				}, &response)
				if err != nil {
					fmt.Println(err)
					return
				}
				err = w.down.Call(stubs.SendRow, stubs.StripContainer{
					Strip: [][]byte{w.strip.Strip[len(w.strip.Strip)-1]},
					Order: 1,
				}, &response)
				if err != nil {
					return
				}
				// Receive top and bottom rows
				w.receiveRow()
				w.receiveRow()
				w.strip.Strip = calculateNextStep(w.strip.Strip)
				fmt.Println(turn)
				w.commandResponse <- stubs.WorkerReport{
					WorkerReturn:    nil,
					CommandExecuted: true,
					Command:         command,
				}
			case stubs.Finish:
				w.commandResponse <- stubs.WorkerReport{
					WorkerReturn:    w.strip,
					CommandExecuted: true,
					Command:         command,
				}
			}
		}
	}
}

func (w *Worker) ExecuteCommand(req stubs.Command, res *stubs.WorkerReport) (err error) {
	w.commands <- req.WorkerCommand
	r := <-w.commandResponse
	res.CommandExecuted = r.CommandExecuted
	res.WorkerReturn = r.WorkerReturn
	res.Command = r.Command
	return
}
func (w *Worker) SendRow(req stubs.StripContainer, res *stubs.StatusReport) (err error) {
	w.rows <- req
	res.Message = "Row received"
	return
}
func (w *Worker) StripReceive(req stubs.StripContainer, res *stubs.WorkerReport) (err error) {
	w.strip = &req
	fmt.Println("Strip Received")
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
	block := make(chan int)
	pAddr := flag.String("ip", "127.0.0.1:8050", "IP and port to listen on")
	brokerAddr := flag.String("broker", "127.0.0.1:8040", "Address of broker instance")
	flag.Parse()
	w := Worker{
		commands:        make(chan stubs.WorkerCommand),
		strip:           nil,
		rows:            make(chan stubs.StripContainer, 2),
		up:              nil,
		down:            nil,
		commandResponse: make(chan stubs.WorkerReport, 2),
		broker:          nil,
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
	go w.workerLoop()
	<-block
}
