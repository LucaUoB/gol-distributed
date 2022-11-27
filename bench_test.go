package main

import (
	"fmt"
	"os"
	"testing"
	"uk.ac.bris.cs/gameoflife/gol"
)

const (
	turns = 100
)

func BenchmarkGOL(b *testing.B) {
	noVis := true
	for i := 1; i <= 6; i++ {
		params := gol.Params{
			Turns:       turns,
			Threads:     i,
			ImageWidth:  512,
			ImageHeight: 512,
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
