package main

import (
	"reflect"
	"github.com/xwb1989/sqlparser"
)

var typeOfBytes = reflect.TypeOf([]byte(nil))
var typeOfStrval = reflect.TypeOf(sqlparser.StrVal(nil))
var typeOfNumVal = reflect.TypeOf(sqlparser.NumVal(nil))
var typeOfSQLNode = reflect.TypeOf((*sqlparser.SQLNode)(nil)).Elem()

type Rewriter func(interface{}) []byte

func Rewrite(node sqlparser.SQLNode, rewriter Rewriter) {
	rewrite(reflect.ValueOf(node), rewriter)
}

func rewrite(nodeVal reflect.Value, rewriter Rewriter) {
	if !nodeVal.IsValid() {
		return
	}
	nodeTyp := nodeVal.Type()
	switch nodeTyp.Kind() {
	case reflect.Slice:
		if nodeTyp == typeOfBytes && !nodeVal.IsNil() {
			val := rewriter(nodeVal.Bytes()) //use rewriter to rewrite the bytes
			nodeVal.SetBytes(val)
		} else if (nodeTyp == typeOfStrval || nodeTyp == typeOfNumVal) && !nodeVal.IsNil() {
			val := rewriter(nodeVal.Interface()) //use rewriter to rewrite the bytes
			copy(nodeVal.Convert(typeOfBytes).Bytes(), val)
		} else if nodeTyp.Implements(typeOfSQLNode) {
			for i := 0; i < nodeVal.Len(); i++ {
				m := nodeVal.Index(i)
				rewrite(m, rewriter)
			}
		}
	case reflect.Struct:
		for i := 0; i < nodeVal.NumField(); i++ {
			f := nodeVal.Field(i)
			rewrite(f, rewriter)
		}
	case reflect.Ptr, reflect.Interface:
		rewrite(nodeVal.Elem(), rewriter)
	}
}
