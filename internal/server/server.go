package server

import (
	"errors"
	"fmt"
	"github.com/hardikroongta8/go_raft/internal/raft"
	"github.com/hardikroongta8/go_raft/internal/utils"
	"github.com/soheilhy/cmux"
	"log"
	"net"
	"sync"
)

type CacheServer struct {
	port           int
	listener       net.Listener
	quitChannel    chan struct{}
	msgChannel     chan Message
	addPeerChannel chan *net.Conn
	peers          map[*Peer]bool

	wg *sync.WaitGroup
	rf *raft.Node
}

func NewCacheServer(port int, id raft.NodeID, raftNodes map[raft.NodeID]string) *CacheServer {
	return &CacheServer{
		port:           port,
		quitChannel:    make(chan struct{}),
		msgChannel:     make(chan Message),
		addPeerChannel: make(chan *net.Conn),
		peers:          make(map[*Peer]bool),
		wg:             new(sync.WaitGroup),
		rf:             raft.NewNode(raftNodes, id),
	}
}

func (s *CacheServer) Start(wg *sync.WaitGroup) {
	defer wg.Done()
	listener, err := net.Listen("tcp", "127.0.0.1:"+fmt.Sprintf("%d", s.port))
	if err != nil {
		fmt.Printf("[Node %d] Error starting the listener: %s\n", s.rf.ID, err.Error())
		return
	}
	mux := cmux.New(listener)
	grpcLn := mux.Match(cmux.HTTP2())
	//grpcLn := mux.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	s.listener = mux.Match(cmux.Any())

	s.wg.Add(4)
	go func() {
		defer s.wg.Done()
		s.acceptConnections()
	}()
	go func() {
		defer s.wg.Done()
		s.mainLoop()
	}()
	go func() {
		defer s.wg.Done()
		s.rf.Start(grpcLn)
	}()
	go func() {
		defer s.wg.Done()
		log.Fatalln(mux.Serve())
	}()

	s.wg.Wait()
}

func (s *CacheServer) mainLoop() {
	for {
		select {
		case <-s.quitChannel:
			return
		case conn := <-s.addPeerChannel:
			s.attachPeer(conn)
		case msg := <-s.msgChannel:
			s.rf.ClientMessageChannel <- string(msg.data)
			res := <-s.rf.ClientResponseChannel
			err := msg.peer.WriteData([]byte(res))
			if err != nil {
				fmt.Printf("[Node %d] Error writing response to client: %s", s.rf.ID, err.Error())
			}
		}
	}
}

func (s *CacheServer) attachPeer(conn *net.Conn) {
	peer := NewPeer(*conn, s.msgChannel)
	s.peers[peer] = true
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err := peer.ReadData()
		if errors.Is(err, utils.ErrConnClosed) {
			return
		}
		if err != nil {
			fmt.Printf("[Node %d] Error while reading data: %s\n", s.rf.ID, err.Error())
		}
		s.peers[peer] = false
	}()
}

func (s *CacheServer) acceptConnections() {
	for {
		select {
		case <-s.quitChannel:
			return
		default:
			conn, err := s.listener.Accept()
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if err != nil {
				fmt.Printf("[Node %d] Error while accepting connection: %s\n", s.rf.ID, err.Error())
				continue
			}
			fmt.Printf("[Node %d] New Connection: %s\n", s.rf.ID, conn.RemoteAddr())
			s.addPeerChannel <- &conn
		}
	}
}

func (s *CacheServer) Quit() {
	var wg sync.WaitGroup
	for peer, connected := range s.peers {
		if !connected {
			continue
		}
		wg.Add(1)
		peer.SendCloseMessage(&wg)
	}
	wg.Wait()
	for peer, connected := range s.peers {
		if !connected {
			continue
		}
		wg.Add(1)
		peer.Close(&wg)
	}
	wg.Wait()
	s.quitChannel <- struct{}{}
	fmt.Printf("[Node %d] Closing TCP Listener...\n", s.rf.ID)
	log.Println("Closing TCP listener...")
	s.rf.Quit()
	err := s.listener.Close()
	if err != nil {
		fmt.Printf("[Node %d] Error closing TCP Listener: %s\n", s.rf.ID, err.Error())
	}
}
