package tags

import (
	"fmt"
	"reflect"
)

func ExtractID(doc any) (string, error) {
	v := reflect.ValueOf(doc)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Tag.Get("whisker") == "id" {
			return fmt.Sprint(v.Field(i).Interface()), nil
		}
	}
	return "", fmt.Errorf("whisker: no field with whisker:\"id\" tag in %s", t.Name())
}

func ExtractVersion(doc any) (int, bool) {
	v := reflect.ValueOf(doc)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Tag.Get("whisker") == "version" {
			return int(v.Field(i).Int()), true
		}
	}
	return 0, false
}

func SetVersion(doc any, version int) {
	v := reflect.ValueOf(doc)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Tag.Get("whisker") == "version" {
			v.Field(i).SetInt(int64(version))
			return
		}
	}
}
