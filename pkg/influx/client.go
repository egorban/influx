package influx

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

type Client struct {
	addr    *net.UDPAddr
	writeCh chan *Point
}

func NewClient(address string) (c *Client, err error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return
	}
	c = new(Client)
	c.addr = addr
	c.writeCh = make(chan *Point)
	go c.sendProc()
	return
}

func (c *Client) WritePoint(point *Point) {
	if point != nil {
		c.writeCh <- point
	}
}

func (c *Client) sendProc() {
	log.Println("influx send proc started")
	for {
		point := <-c.writeCh
		line := point.toLine()
		if line != "" {
			c.send(line)
		}

	}
}

func (c *Client) send(line string) {
	log.Println("influx write proc start")
	conn, err := net.DialUDP("udp", nil, c.addr)
	if nil != err {
		log.Println("influx error connect", err)
	}
	defer conn.Close()
	w := bufio.NewWriter(conn)
	log.Println("influx send line", line)
	_, err = fmt.Fprintf(w, line)
	if nil != err {
		log.Println("influx error send metrics", err)
	}
	err = w.Flush()
	if nil != err {
		log.Println("influx error flush", err)
	}

}
