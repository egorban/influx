package influx

import (
	"log"
	"strconv"
	"strings"
)

type Tags map[string]string

type Values map[string]interface{}

type Point struct {
	table  string
	tags   Tags
	values Values
}

func NewPoint(table string, tags map[string]string, values map[string]interface{}) (p *Point) {
	if table == "" || len(values) <= 0 {
		return
	}
	p = new(Point)
	p.table = table
	p.tags = tags
	p.values = values
	return
}

func (p *Point) toLine() (line string) {
	if p.table == "" {
		return
	}
	values := p.values.toLine()
	if values == "" {
		return
	}
	tags := p.tags.toLine()
	line = p.table + "," + tags + " " + values + "\x0a"
	return
}

func (tags Tags) toLine() (line string) {
	if len(tags) <= 0 {
		return
	}
	for key, value := range tags {
		if key != "" && value != "" {
			line = line + key + "=" + value + ","
		}
	}
	line = strings.TrimSuffix(line, ",")
	return
}

func (values Values) toLine() (line string) {
	if len(values) <= 0 {
		return
	}
	for key, value := range values {
		v := convertValue(value)
		if key != "" && v != "" {
			line = line + key + "=" + v + ","
		}
	}
	line = strings.TrimSuffix(line, ",")
	return
}

func convertValue(v interface{}) string {
	switch v := v.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	default:
		log.Println("influx error convert value")
		return ""
	}
}
