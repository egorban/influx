package influx

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type Client struct {
	addr          *net.UDPAddr
	bufferCh      chan *Point
	writeCh       chan []string
	writeBuffer   []string
	logger        *logrus.Entry
	flushInterval uint
	batchSize     uint
}

func NewClient(address string, flushInterval uint, batchSize uint) (c *Client, err error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return
	}
	c = new(Client)
	c.addr = addr
	c.bufferCh = make(chan *Point)
	c.writeCh = make(chan []string)
	c.writeBuffer = make([]string, 0, batchSize+1)
	c.logger = logrus.WithFields(logrus.Fields{"type": "monitoring", "client": "influx"})
	c.flushInterval = flushInterval
	c.batchSize = batchSize
	go c.bufferProc()
	go c.writeProc()
	return
}

func (c *Client) WritePoint(point *Point) {
	if point != nil {
		c.bufferCh <- point
	}
}

func (c *Client) bufferProc() {
	c.logger.Println("buffer proc started")
	ticker := time.NewTicker(time.Duration(c.flushInterval))
	for {
		select {
		case point := <-c.bufferCh:
			line := point.toLine()
			if line != "" {
				c.writeBuffer = append(c.writeBuffer, line)
				if len(c.writeBuffer) == int(c.batchSize) {
					c.flushBuffer()
				}
			}
		case <-ticker.C:
			if len(c.writeBuffer) > 0 {
				c.flushBuffer()
			}
		}
	}
}

func (c *Client) flushBuffer() {
	c.writeCh <- c.writeBuffer
	c.writeBuffer = []string(nil)
}

func (c *Client) writeProc() {
	c.logger.Infof("write proc start")
	for {
		batch := <-c.writeCh
		conn, err := net.DialUDP("udp", nil, c.addr)
		if nil != err {
			c.logger.Errorf("error connect influx %s:", err)
		}
		defer conn.Close()
		w := bufio.NewWriter(conn)
		c.logger.Infof("send batch %v", batch)
		_, err = fmt.Fprintf(w, strings.Join(batch, ""))
		if nil != err {
			c.logger.Errorf("error send metrics to influx %s:", err)
		}
		err = w.Flush()
		if nil != err {
			c.logger.Errorf("error flush %s:", err)
		}
	}
}
