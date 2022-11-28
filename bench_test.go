package main

import (
	"fmt"
	"os"
	"testing"
	"uk.ac.bris.cs/gameoflife/gol"
)

const (
	turns = 15
)

func BenchmarkGOL(b *testing.B) {
	noVis := true
	for i := 6; i >= 1; i-- {
		params := gol.Params{
			Turns:       turns,
			Threads:     i,
			ImageWidth:  5120,
			ImageHeight: 5120,
		}
		b.Run(fmt.Sprintf("%d_%d", i, turns), func(b *testing.B) {
			os.Stdout = nil
			for f := 0; f < b.N; f++ {
				b.StartTimer()
				runGOLBench(params, &noVis)
				b.StopTimer()
			}
		})
	}
}
