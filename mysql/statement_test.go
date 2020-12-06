// Go MSKeeper Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2020 The MSKeeper Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"encoding/json"
	"log"
	"testing"
)

func TestConvertValues(t *testing.T) {

	nargs, err := converter{}.ConvertValues([]interface{}{"1", 2, []byte{3}, 4.4})
	if err != nil {
		t.Fatalf("ConvertValues failed %v", err)
	}
	for i := 0; i < len(nargs); i++ {
		log.Printf("nargs[%v]: %v %T", i, nargs[i], nargs[i])
	}

	_, ok := nargs[0].(string)
	if !ok {
		t.Fatalf("Convert of %v failed ", nargs[0])
	}
	_, ok = nargs[1].(int64)
	if !ok {
		t.Fatalf("Convert of %v failed ", nargs[1])
	}
	_, ok = nargs[2].([]uint8)
	if !ok {
		t.Fatalf("Convert of %v failed ", nargs[2])
	}
	_, ok = nargs[3].(float64)
	if !ok {
		t.Fatalf("Convert of %v failed ", nargs[3])
	}
}

func TestConvertDerivedString(t *testing.T) {
	type derived string

	output, err := converter{}.ConvertValue(derived("value"))
	if err != nil {
		t.Fatal("Derived string type not convertible", err)
	}

	if output != "value" {
		t.Fatalf("Derived string type not converted, got %#v %T", output, output)
	}
}

func TestConvertDerivedByteSlice(t *testing.T) {
	type derived []uint8

	output, err := converter{}.ConvertValue(derived("value"))
	if err != nil {
		t.Fatal("Byte slice not convertible", err)
	}

	if bytes.Compare(output.([]byte), []byte("value")) != 0 {
		t.Fatalf("Byte slice not converted, got %#v %T", output, output)
	}
}

func TestConvertDerivedUnsupportedSlice(t *testing.T) {
	type derived []int

	_, err := converter{}.ConvertValue(derived{1})
	if err == nil || err.Error() != "unsupported type mysql.derived, a slice of int" {
		t.Fatal("Unexpected error", err)
	}
}

func TestConvertDerivedBool(t *testing.T) {
	type derived bool

	output, err := converter{}.ConvertValue(derived(true))
	if err != nil {
		t.Fatal("Derived bool type not convertible", err)
	}

	if output != true {
		t.Fatalf("Derived bool type not converted, got %#v %T", output, output)
	}
}

func TestConvertPointer(t *testing.T) {
	str := "value"

	output, err := converter{}.ConvertValue(&str)
	if err != nil {
		t.Fatal("Pointer type not convertible", err)
	}

	if output != "value" {
		t.Fatalf("Pointer type not converted, got %#v %T", output, output)
	}
}

func TestConvertSignedIntegers(t *testing.T) {
	values := []interface{}{
		int8(-42),
		int16(-42),
		int32(-42),
		int64(-42),
		int(-42),
	}

	for _, value := range values {
		output, err := converter{}.ConvertValue(value)
		if err != nil {
			t.Fatalf("%T type not convertible %s", value, err)
		}

		if output != int64(-42) {
			t.Fatalf("%T type not converted, got %#v %T", value, output, output)
		}
	}
}

func TestConvertUnsignedIntegers(t *testing.T) {
	values := []interface{}{
		uint8(42),
		uint16(42),
		uint32(42),
		uint64(42),
		uint(42),
	}

	for _, value := range values {
		output, err := converter{}.ConvertValue(value)
		if err != nil {
			t.Fatalf("%T type not convertible %s", value, err)
		}

		if output != uint64(42) {
			t.Fatalf("%T type not converted, got %#v %T", value, output, output)
		}
	}

	output, err := converter{}.ConvertValue(^uint64(0))
	if err != nil {
		t.Fatal("uint64 high-bit not convertible", err)
	}

	if output != ^uint64(0) {
		t.Fatalf("uint64 high-bit converted, got %#v %T", output, output)
	}
}

func TestConvertJSON(t *testing.T) {
	raw := json.RawMessage("{}")

	out, err := converter{}.ConvertValue(raw)

	if err != nil {
		t.Fatal("json.RawMessage was failed in convert", err)
	}

	if _, ok := out.(json.RawMessage); !ok {
		t.Fatalf("json.RawMessage converted, got %#v %T", out, out)
	}
}
