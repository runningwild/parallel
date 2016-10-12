package parallel_test

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/runningwild/parallel"

	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestRunN(t *testing.T) {
	Convey("RunN", t, func() {
		Convey("panics on invalid parameters", func() {
			So(func() { parallel.RunN(-1, func() {}) }, ShouldPanic)
			So(func() { parallel.RunN(10, func() {}) }, ShouldNotPanic)
			So(func() { parallel.RunN(10, func(n int) {}, 4) }, ShouldNotPanic)
			So(func() { parallel.RunN(10, 5) }, ShouldPanic)
			So(func() { parallel.RunN(10, func(n int) {}, "foo") }, ShouldPanic)
			So(func() { parallel.RunN(10, func(n int) {}, 2, 3) }, ShouldPanic)
		})
		Convey("actually runs as many things in parallel as requested", func() {
			// This checks that we're actually running N things.
			c := make(chan struct{})
			block := make(chan struct{})
			N := 1000
			go parallel.RunN(N, func(c, block chan struct{}) {
				c <- struct{}{}
				<-block
			}, c, block)
			expire := time.After(time.Second)
			for i := 0; i < N; i++ {
				select {
				case <-c:
				case <-expire:
					panic("failed to hear back from each parallel function")
				}
			}
			close(block)

			// Simple check, racy but not too bad.  Makes sure that all of the things run in parallel.
			start := time.Now()
			parallel.RunN(N, time.Sleep, 100*time.Millisecond)
			So(time.Since(start), ShouldBeGreaterThan, 100*time.Millisecond)
			So(time.Since(start), ShouldBeLessThan, 500*time.Millisecond)
		})
	})
}

func TestRunNK(t *testing.T) {
	Convey("RunNK", t, func() {
		Convey("panics on invalid parameters", func() {
			So(func() { parallel.RunNK(-1, 2, func() {}) }, ShouldPanic)
			So(func() { parallel.RunNK(2, -1, func() {}) }, ShouldPanic)
			So(func() { parallel.RunNK(10, 2, func() {}) }, ShouldNotPanic)
			So(func() { parallel.RunNK(10, 1, func(n int) {}, 4) }, ShouldNotPanic)
			So(func() { parallel.RunNK(10, 5, 3) }, ShouldPanic)
			So(func() { parallel.RunNK(10, 1, func(n int) {}, "foo") }, ShouldPanic)
			So(func() { parallel.RunNK(10, 1, func(n int) {}, 2, 3) }, ShouldPanic)
		})
		Convey("runs f n times", func() {
			var total int32
			parallel.RunNK(1000, 10, atomic.AddInt32, &total, int32(1))
			So(total, ShouldEqual, 1000)
		})
		Convey("runs f up to k times in parallel", func() {
			c := make(chan struct{})
			block := make(chan struct{})
			N, K := 1000, 450
			go parallel.RunNK(N, K, func(c, block chan struct{}) {
				c <- struct{}{}
				<-block
			}, c, block)
			expire := time.After(time.Second)
			for i := 0; i < K; i++ {
				select {
				case <-c:
				case <-expire:
					panic("failed to hear back from each parallel function")
				}
			}
			close(block)
			for i := 0; i < N-K; i++ {
				<-c
			}
		})
	})
}

func TestFunnel(t *testing.T) {
	Convey("Funnel", t, func() {
		Convey("panics on invalid parameters", func() {
			So(func() { parallel.Funnel(0) }, ShouldPanic)
			So(func() { parallel.Funnel(make(chan int)) }, ShouldPanic)
			So(func() { parallel.Funnel(make(chan<- int)) }, ShouldPanic)
			So(func() { parallel.Funnel(make(<-chan int)) }, ShouldPanic)
			So(func() { parallel.Funnel(make(chan chan int)) }, ShouldPanic)
			So(func() { parallel.Funnel(make(chan chan<- int)) }, ShouldPanic)
			So(func() { parallel.Funnel(make(chan (<-chan int))) }, ShouldNotPanic)
			So(func() { parallel.Funnel(make(chan<- chan int)) }, ShouldPanic)
			So(func() { parallel.Funnel(make(chan<- chan<- int)) }, ShouldPanic)
			So(func() { parallel.Funnel(make(chan<- <-chan int)) }, ShouldPanic)
			So(func() { parallel.Funnel(make(<-chan chan int)) }, ShouldPanic)
			So(func() { parallel.Funnel(make(<-chan chan<- int)) }, ShouldPanic)
			So(func() { parallel.Funnel(make(<-chan <-chan int)) }, ShouldNotPanic)
		})
		Convey("closes immediately when there are no inputs", func() {
			c := make(chan (<-chan int))
			r := parallel.Funnel(c).(<-chan int)
			close(c)
			<-r
		})
		Convey("acts like a pipe when there is only one channel involved", func() {
			c := make(chan (<-chan int))
			r := parallel.Funnel(c).(<-chan int)
			g := make(chan int)
			c <- g
			close(c)
			go func() {
				g <- 0
				g <- 1
				g <- 2
				close(g)
			}()
			So(<-r, ShouldEqual, 0)
			So(<-r, ShouldEqual, 1)
			So(<-r, ShouldEqual, 2)
			_, ok := <-r
			So(ok, ShouldBeFalse)
		})
		Convey("interleaves inputs in a valid way", func() {
			c := make(chan (<-chan int))
			r := parallel.Funnel(c).(<-chan int)
			N := 100
			go func() {
				defer close(c)
				for i := 0; i < N; i++ {
					m := make(chan int)
					c <- m
					go func(i int) {
						defer close(m)
						for j := 0; j < N; j++ {
							m <- i*100 + j
						}
					}(i)
				}
			}()
			m := make([][]int, N)
			for v := range r {
				m[v/N] = append(m[v/N], v)
			}
			for i := range m {
				So(sort.IsSorted(sort.IntSlice(m[i])), ShouldBeTrue)
			}
		})
	})
}

func TestPipe(t *testing.T) {
	Convey("Pipe", t, func() {
		Convey("panics on invalid parameters", func() {
			c := make(chan int)
			So(func() { parallel.Pipe((chan int)(c), nil) }, ShouldNotPanic)
			So(func() { parallel.Pipe((chan int)(c), func(int) bool { return true }) }, ShouldNotPanic)
			So(func() { parallel.Pipe((<-chan int)(c), func(int) bool { return true }) }, ShouldNotPanic)
			So(func() { parallel.Pipe((chan int)(c), func(int, int) bool { return true }) }, ShouldPanic)
			So(func() { parallel.Pipe((chan int)(c), func(int) {}) }, ShouldPanic)
			So(func() { parallel.Pipe((chan int)(c), func(float64) bool { return true }) }, ShouldPanic)
			So(func() { parallel.Pipe((chan int)(c), func(*int) bool { return true }) }, ShouldPanic)
			close(c)
		})
		Convey("pipes all the values through untouched if no function is provided", func() {
			in := make(chan int)
			out := parallel.Pipe(in, nil).(<-chan int)
			for i := 0; i < 10; i++ {
				in <- i
			}
			close(in)
			var vals []int
			for v := range out {
				vals = append(vals, v)
			}
			So(vals, ShouldResemble, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
		})
		Convey("pipes all the values through the provided function", func() {
			in := make(chan int)
			out := parallel.Pipe(in, func(n int) string { return fmt.Sprintf("%d", n) }).(<-chan string)
			for i := 0; i < 10; i++ {
				in <- i
			}
			close(in)
			var vals []string
			for v := range out {
				vals = append(vals, v)
			}
			So(vals, ShouldResemble, []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"})
		})
	})
}

func TestDrain(t *testing.T) {
	Convey("Drain", t, func() {
		Convey("panics on invalid parameters", func() {
			c := make(chan int)
			close(c)
			So(func() { parallel.Drain((chan int)(c)) }, ShouldNotPanic)
			So(func() { parallel.Drain((<-chan int)(c)) }, ShouldNotPanic)
			So(func() { parallel.Drain((chan<- int)(c)) }, ShouldPanic)
			So(func() { parallel.Drain(2) }, ShouldPanic)
			So(func() { parallel.Drain([]chan int{make(chan int), make(chan int)}) }, ShouldPanic)
		})
		Convey("drains all values from a channel", func() {
			c := make(chan int)
			done := make(chan struct{})
			go func() {
				defer close(done)
				parallel.Drain(c)
			}()

			// Because the drain is active we should be able to send as much as we want
			for i := 0; i < 100; i++ {
				c <- i
			}

			select {
			case <-done:
				t.Errorf("shouldn't have been able to read from done channel, input channel hasn't closed yet.")
			default:
			}

			close(c)
			<-done
		})
	})
}

func BenchmarkPipe(b *testing.B) {
	in := make(chan int)
	out := parallel.Pipe(in, nil).(<-chan int)
	go func() {
		defer close(in)
		for i := 0; i < b.N; i++ {
			in <- i
		}
	}()
	for range out {
	}
}

func BenchmarkPipeWithFunction(b *testing.B) {
	in := make(chan int)
	out := parallel.Pipe(in, func(n int) int { return n + 1 }).(<-chan int)
	go func() {
		defer close(in)
		for i := 0; i < b.N; i++ {
			in <- i
		}
	}()
	for range out {
	}
}

func BenchmarkRawPipe(b *testing.B) {
	in := make(chan int)
	out := make(chan int)
	go func() {
		defer close(in)
		for i := 0; i < b.N; i++ {
			in <- i
		}
	}()
	go func() {
		defer close(out)
		for v := range in {
			out <- v
		}
	}()
	for range out {
	}
}
