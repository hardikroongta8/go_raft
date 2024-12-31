package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/hardikroongta8/go_raft/internal/utils"

	"log"
	"net"
	"sync"
)

type Client struct {
	addr string
	conn net.Conn
	WG   *sync.WaitGroup
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr: addr,
		conn: conn,
		WG:   new(sync.WaitGroup),
	}, nil
}

func (c *Client) ReadData() error {
	for {
		reader := utils.NewReader(c.conn)
		data, err := reader.Read()
		if errors.Is(err, net.ErrClosed) || errors.Is(err, utils.ErrConnClosed) {
			return nil
		}
		if err != nil {
			return err
		}
		log.Println(string(data))
	}
}

func (c *Client) Put(ctx context.Context, key string, value string) error {
	str := fmt.Sprintf("PUT\r\n%s\r\n%s\r\n", key, value)
	writer := utils.NewWriter(c.conn)
	return writer.Write([]byte(str))
}

func (c *Client) Get(ctx context.Context, key string) error {
	str := fmt.Sprintf("GET\r\n%s\r\n", key)
	writer := utils.NewWriter(c.conn)
	return writer.Write([]byte(str))
}

func (c *Client) SendCloseMessage(wg *sync.WaitGroup) {
	defer wg.Done()
	w := utils.NewWriter(c.conn)
	err := w.Write([]byte(""))
	if err != nil {
		log.Println("Error sending close signal to client:", err.Error())
		return
	}
}

func (c *Client) Close() error {
	writer := utils.NewWriter(c.conn)
	err := writer.Write([]byte(""))
	if err != nil {
		return err
	}
	return c.conn.Close()
}

func main() {
	port := flag.Int("p", 8081, "port")
	flag.Parse()
	log.Println("Connecting to server at port:", *port)
	c, err := NewClient("127.0.0.1:" + fmt.Sprintf("%d", *port))
	if err != nil {
		log.Fatalln("Client Error:", err.Error())
	}
	c.WG.Add(1)
	go func() {
		err := c.ReadData()
		if err != nil {
			log.Println("Error reading data:", err.Error())
		}
		c.WG.Done()
	}()

	err = c.Put(context.Background(), "name", fmt.Sprintf("Hardik"))
	if err != nil {
		log.Println("Client Error:", err.Error())
	}

	err = c.Put(context.Background(), "surname", fmt.Sprintf("Roongta"))
	if err != nil {
		log.Println("Client Error:", err.Error())
	}

	err = c.Put(context.Background(), "city", fmt.Sprintf("Guwahati"))
	if err != nil {
		log.Println("Client Error:", err.Error())
	}

	err = c.Put(context.Background(), "clg", "iitg")
	if err != nil {
		log.Println("Client Error:", err.Error())
	}

	err = c.Get(context.Background(), "name")
	if err != nil {
		log.Println("Client Error:", err.Error())
	}

	err = c.Put(context.Background(), "color", fmt.Sprintf("Red"))
	if err != nil {
		log.Println("Client Error:", err.Error())
	}

	err = c.Get(context.Background(), "color")
	if err != nil {
		log.Println("Client Error:", err.Error())
	}

	err = c.Get(context.Background(), "surname")
	if err != nil {
		log.Println("Client Error:", err.Error())
	}

	c.WG.Wait()
}