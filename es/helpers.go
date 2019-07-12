package es

import (
	"reflect"
	"strings"
	"time"
)

// GetTypeName of given struct
func GetTypeName(source interface{}) (reflect.Type, string) {
	rawType := reflect.TypeOf(source)

	// source is a pointer, convert to its value
	if rawType.Kind() == reflect.Ptr {
		rawType = rawType.Elem()
	}

	name := rawType.String()
	// we need to extract only the name without the package
	// name currently follows the format `package.StructName`
	parts := strings.Split(name, ".")
	return rawType, parts[1]
}

// GetTimestamp will get the current timestamp
func GetTimestamp() time.Time {
	return time.Now() // TODO make this changable. Use ambient context type of thing
}
