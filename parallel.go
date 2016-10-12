package parallel

import (
	"fmt"
	"reflect"
	"sync"
)

// RunN runs the function f n times in parallel and waits until each is complete to return.
// args are passed as the arguments to f.  The types for each must match of RunN will panic.
// Return values from f are ignored.
func RunN(n int, f interface{}, args ...interface{}) {
	if n < 0 {
		panic("cannot RunN with n < 0")
	}
	typ := reflect.TypeOf(f)
	if typ.Kind() != reflect.Func {
		panic(fmt.Sprintf("got type %v, expected %v", typ.Kind(), reflect.Func))
	}
	if typ.NumIn() != len(args) {
		panic(fmt.Sprintf("got %d inputs, expected %d", typ.NumIn(), len(args)))
	}
	for i := range args {
		if !reflect.TypeOf(args[i]).AssignableTo(typ.In(i)) {
			panic(fmt.Sprintf("%v cannot be assigned to %v", reflect.TypeOf(args[i]), typ.In(i)))
		}
	}
	vals := make([]reflect.Value, len(args))
	for i := range args {
		vals[i] = reflect.ValueOf(args[i])
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reflect.ValueOf(f).Call(vals)
		}()
	}
	wg.Wait()
}

// RunNK runs the function f n times, with at most k running in parallel at any time, and waits
// until each is complete to return.  args are passed as the arguments to f.  The types for each
// must match of RunNK will panic.  Return values from f are ignored.
func RunNK(n, k int, f interface{}, args ...interface{}) {
	if k < 0 || n < 0 {
		panic("cannot RunNK with k < 0 || n < 0")
	}
	typ := reflect.TypeOf(f)
	if typ.Kind() != reflect.Func {
		panic(fmt.Sprintf("got type %v, expected %v", typ.Kind(), reflect.Func))
	}
	if typ.NumIn() != len(args) {
		panic(fmt.Sprintf("got %d inputs, expected %d", typ.NumIn(), len(args)))
	}
	for i := range args {
		if !reflect.TypeOf(args[i]).AssignableTo(typ.In(i)) {
			panic(fmt.Sprintf("%v cannot be assigned to %v", reflect.TypeOf(args[i]), typ.In(i)))
		}
	}
	vals := make([]reflect.Value, len(args))
	for i := range args {
		vals[i] = reflect.ValueOf(args[i])
	}

	limit := make(chan struct{}, k)
	for i := 0; i < k; i++ {
		limit <- struct{}{}
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-limit
			reflect.ValueOf(f).Call(vals)
			limit <- struct{}{}
		}()
	}
	wg.Wait()
}

// Funnel has an effective signature of Funnel(c <-chan <-chan T) <-chan T.  It receives channels on
// the receive channel c and for each one launches a go-routine to read values of type T off of that
// channel.  All of the values read will be sent along the returned channel.  When the input channel
// is closed and all received channels are closed then the returned channel will also be closed.
// The net effect of this is that the Funnel will collect all of the incoming values on all of the
// channels received and send them along a single channel and close only when everything else has
// been closed.
func Funnel(in interface{}) interface{} {
	typ0 := reflect.TypeOf(in)
	if typ0.Kind() != reflect.Chan || typ0.ChanDir() == reflect.SendDir {
		// BothDir is ok here, because the type system would normally cast it to a receive-only chan
		// we also want to be ok with that, so we only panic on a send-only chan.
		panic("can only call Funnel on a receive channel of receive channels")
	}
	typ1 := typ0.Elem()
	if typ1.Kind() != reflect.Chan || typ1.ChanDir() != reflect.RecvDir {
		// Here this needs to be a receive-only chan, because the type system would have complained
		// about it otherwise, and we're trying to emulate what the type system would do.
		panic("can only call Funnel on a receive channel of receive channels")
	}
	typ2 := typ1.Elem()

	output := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, typ2), 0)
	ret := output.Convert(reflect.ChanOf(reflect.RecvDir, typ2))
	var wg sync.WaitGroup
	go func() {
		ccv := reflect.ValueOf(in)
		// This is long-hand for a range over ccv
		for cv, ok := ccv.Recv(); ok; cv, ok = ccv.Recv() {
			wg.Add(1)
			go func(cv reflect.Value) {
				defer wg.Done()
				// This is long-hand for a range over cv
				for v, ok := cv.Recv(); ok; v, ok = cv.Recv() {
					output.Send(v)
				}
			}(cv)
		}
		wg.Wait()
		output.Close()
	}()
	return ret.Interface()
}

// Pipe has an effective signature of Pipe(c <-chan T, f func(T) S) <-chan S.  c is a receive
// channel of type T and a function that maps type T to type S.  It reads from c until it is closed,
// puts each value it gets into f, then sends the result along the returned channel.  The returned
// channel will be closed after all values have been received from c, transformed, and sent.
// Pipe will buffer indefinitely.  If f is nil, it will pass values through directly.
func Pipe(in interface{}, f interface{}) interface{} {
	typIn := reflect.TypeOf(in)
	if typIn.Kind() != reflect.Chan || typIn.ChanDir() == reflect.SendDir {
		// A bidirectional channel is ok, the type system would convert it, but we aren't going to
		// send on it so it's ok either way.
		panic("first parameter of Pipe must be a receiveable channel")
	}

	typOut := typIn.Elem()
	if f != nil {
		typF := reflect.TypeOf(f)
		if typF.Kind() != reflect.Func || typF.NumIn() != 1 || typF.NumOut() != 1 {
			panic("second parameter of Pipe must be nil, or a function that takes one value and returns one value")
		}
		if typF.In(0).Kind() != typIn.Elem().Kind() {
			panic("Pipe's channel parameter's type must be the same as the input to the function")
		}
		typOut = typF.Out(0)
	}
	out := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, typOut), 0)

	go func() {
		defer out.Close()
		var buffer []reflect.Value
		// Receive from in, and send to out if there is anything in the buffer.
		noSend := reflect.Zero(reflect.TypeOf(out.Interface()))
		zeroSend := reflect.Zero(typOut)
		cases := []reflect.SelectCase{
			reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(in),
			},
			reflect.SelectCase{
				Dir:  reflect.SelectSend,
				Chan: noSend,
				Send: zeroSend,
			},
		}
		for !cases[0].Chan.IsNil() || len(buffer) > 0 {
			if len(buffer) == 0 {
				cases[1].Chan = noSend
				cases[1].Send = zeroSend
			} else {
				cases[1].Chan = out
				cases[1].Send = buffer[0]
			}
			index, value, ok := reflect.Select(cases)
			switch index {
			case 0:
				if !ok {
					cases[0].Chan = reflect.Zero(cases[0].Chan.Type())
					continue
				}
				if f == nil {
					buffer = append(buffer, value)
					continue
				}
				buffer = append(buffer, reflect.ValueOf(f).Call([]reflect.Value{value})[0])
			case 1:
				buffer = buffer[1:]
			}
		}
	}()

	return out.Convert(reflect.ChanOf(reflect.RecvDir, typOut)).Interface()
}

// Drain has an effetive signature of Drain(c <-chan T).  It will read from c until c is closed.
func Drain(c interface{}) {
	typC := reflect.TypeOf(c)
	if typC.Kind() != reflect.Chan || typC.ChanDir() == reflect.SendDir {
		// BothDir is ok here, because the type system would normally cast it to a receive-only chan
		// we also want to be ok with that, so we only panic on a send-only chan.
		panic("can only call Drain on a receive channel")
	}
	cv := reflect.ValueOf(c)
	for {
		if _, ok := cv.Recv(); !ok {
			break
		}
	}
}
