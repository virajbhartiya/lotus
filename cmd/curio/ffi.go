package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"reflect"

	"github.com/samber/lo"
	"github.com/urfave/cli/v2"

	ffidirect "github.com/filecoin-project/lotus/lib/ffiselect/ffidirect"

	"github.com/filecoin-project/lotus/lib/ffiselect"
)

var ffiCmd = &cli.Command{
	Name:   "ffi",
	Hidden: true,
	Flags: []cli.Flag{
		layersFlag,
	},
	Action: func(cctx *cli.Context) (err error) {
		output := os.NewFile(uintptr(3), "out")

		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
			}
			if err != nil {
				err = gob.NewEncoder(output).Encode(ffiselect.ValErr{Val: nil, Err: err})
				if err != nil {
					panic(err)
				}
			}
		}()
		var callInfo ffiselect.FFICall
		if err := gob.NewDecoder(os.Stdin).Decode(&callInfo); err != nil {
			return err
		}

		args := lo.Map(callInfo.Args, func(arg any, i int) reflect.Value {
			return reflect.ValueOf(arg)
		})

		// All methods 1st arg is a context, not passed in.
		args = append([]reflect.Value{reflect.ValueOf(cctx.Context)}, args...)

		resAry := reflect.ValueOf(ffidirect.FFI{}).MethodByName(callInfo.Fn).Call(args)
		res := lo.Map(resAry, func(res reflect.Value, i int) any {
			return res.Interface()
		})

		return gob.NewEncoder(output).Encode(ffiselect.ValErr{Val: res, Err: nil})
	},
}
