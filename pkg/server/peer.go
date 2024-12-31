package server

import (
	"fmt"
	"github.com/hardikroongta8/go_raft/internal/utils"
	"io"
	"log"
	"net"
	"sync"
)

type Peer struct {
	conn       net.Conn
	msgChannel chan Message
}

func NewPeer(conn net.Conn, m chan Message) *Peer {
	return &Peer{
		conn:       conn,
		msgChannel: m,
	}
}

type Message struct {
	data []byte
	peer *Peer
}

func (p *Peer) ReadData() error {
	for {
		parser := utils.NewReader(p.conn)
		data, err := parser.Read()
		log.Println("DATA:", string(data))
		if err == io.EOF {
			log.Println("EOF Error:", err.Error())
			return nil
		}
		if err != nil {
			log.Println("NON EOF Error:", err.Error())
			continue
		}
		log.Println("I AM HERE")
		p.msgChannel <- Message{
			data: data,
			peer: p,
		}
	}
}

func (p *Peer) WriteData(data []byte) error {
	w := utils.NewWriter(p.conn)
	return w.Write(data)
}

func (p *Peer) SendCloseMessage(wg *sync.WaitGroup) {
	defer wg.Done()
	err := p.WriteData([]byte(""))
	if err != nil {
		fmt.Println("Error sending CLOSE signal to client:", err.Error())
		return
	}
}

func (p *Peer) Close(wg *sync.WaitGroup) {
	defer wg.Done()
	err := p.conn.Close()
	if err != nil {
		fmt.Println("Error closing connection:", p.conn.RemoteAddr())
		return
	}
}