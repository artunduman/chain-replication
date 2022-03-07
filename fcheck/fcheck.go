/*

This package specifies the API to the failure checking library to be
used in assignment 2 of UBC CS 416 2021W2.

You are *not* allowed to change the API below. For example, you can
modify this file by adding an implementation to Stop, but you cannot
change its API.

*/

package fchecker

import (
	"bytes"
	"encoding/gob"
	"errors"
	"net"
	"sync"
	"time"
)

////////////////////////////////////////////////////// DATA
// Define the message types fchecker has to use to communicate to other
// fchecker instances. We use Go's type declarations for this:
// https://golang.org/ref/spec#Type_declarations

// Heartbeat message.
type HBeatMessage struct {
	EpochNonce uint64 // Identifies this fchecker instance/epoch.
	SeqNum     uint64 // Unique for each heartbeat in an epoch.
}

// An ack message; response to a heartbeat.
type AckMessage struct {
	HBEatEpochNonce uint64 // Copy of what was received in the heartbeat.
	HBEatSeqNum     uint64 // Copy of what was received in the heartbeat.
}

// Notification of a failure, signal back to the client using this
// library.
type FailureDetected struct {
	UDPIpPort string // The RemoteIP:RemotePort of the failed node.
	ServerId  uint8
	Timestamp time.Time // The time when the failure was detected.
}

////////////////////////////////////////////////////// API

type Server struct {
	ServerId uint8
	Addr     string
}

type StartStruct struct {
	AckLocalIPAckLocalPort       string
	EpochNonce                   uint64
	HBeatLocalIP                 string
	HBeatRemoteIPHBeatRemotePort []Server
	LostMsgThresh                uint8
}

type readStruct struct {
	from    net.Addr
	len     int
	err     error
	recvBuf []byte
}

var isActive bool = false

var stopAck chan bool = make(chan bool)
var stopHBeatMap sync.Map
var notifyCh chan FailureDetected

// Starts the fcheck library.
func Start(arg StartStruct) (<-chan FailureDetected, error) {
	if isActive {
		return nil, errors.New("fcheck is already monitoring")
	}

	if arg.HBeatLocalIP == "" {
		err := ackInit(arg)

		if err != nil {
			return nil, err
		}

		isActive = true

		return nil, nil
	}

	if arg.AckLocalIPAckLocalPort != "" {
		err := ackInit(arg)

		if err != nil {
			return nil, err
		}
	}

	err := hbeatInit(arg)

	if err != nil {
		writeStopChannels()

		return nil, err
	}

	isActive = true

	return notifyCh, nil
}

// Tells the library to stop monitoring/responding acks.
func Stop() {
	if isActive {
		writeStopChannels()

		close(notifyCh)

		isActive = false
	}
}

func writeStopChannels() {
	stopAck <- true

	stopHBeatMap.Range(func(key, value interface{}) bool {
		value.(chan bool) <- true
		return true
	})
}

func ackInit(arg StartStruct) error {
	laddr, err := net.ResolveUDPAddr("udp", arg.AckLocalIPAckLocalPort)

	if err != nil {
		return errors.New("failed to resolve UDP address")
	}

	conn, err := net.ListenUDP("udp", laddr)

	if err != nil {
		return errors.New("failed to listen")
	}

	go ack(conn)

	return nil
}

func hbeatInit(arg StartStruct) error {
	notifyCh = make(chan FailureDetected, len(arg.HBeatRemoteIPHBeatRemotePort))

	for _, server := range arg.HBeatRemoteIPHBeatRemotePort {
		laddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(arg.HBeatLocalIP, "0"))
		raddr, err := net.ResolveUDPAddr("udp", server.Addr)

		if err != nil {
			return errors.New("failed to resolve UDP address")
		}

		conn, err := net.DialUDP("udp", laddr, raddr)

		if err != nil {
			return errors.New("failed to dial")
		}

		stopHBeatMap.Store(server.ServerId, make(chan bool))
		go hbeat(arg, server, conn)
	}

	return nil
}

func hbeat(arg StartStruct, server Server, conn *net.UDPConn) {
	defer conn.Close()

	seqNumMap := make(map[uint64]time.Time)

	var rs readStruct
	rs.recvBuf = make([]byte, 1024)

	seqNum := uint64(0)
	rtt := time.Duration(3) * time.Second
	numLost := uint8(0)

	hBeatInterace, _ := stopHBeatMap.Load(server.ServerId)
	stopHBeat := hBeatInterace.(chan bool)

	for {
		hbeatMessage := HBeatMessage{
			EpochNonce: arg.EpochNonce,
			SeqNum:     seqNum,
		}

		seqNumMap[seqNum] = time.Now()
		conn.Write(encodeHBeatMessage(&hbeatMessage))
		deadline := time.Now().Add(rtt)

	InnerLoop:
		for {
			select {
			case <-stopHBeat:
				stopHBeatMap.Delete(server.ServerId)
				return
			case <-handleRead(&rs, deadline, conn):
				if rs.err != nil {
					numLost++
					break InnerLoop
				} else {
					ackMessage, _ := decodeAckMessage(rs.recvBuf, rs.len)

					if ackMessage.HBEatEpochNonce != arg.EpochNonce {
						continue InnerLoop
					}

					numLost = 0

					if sentTime, ok := seqNumMap[ackMessage.HBEatSeqNum]; ok {
						rtt = (rtt + time.Since(sentTime)) / 2
						delete(seqNumMap, ackMessage.HBEatSeqNum)
					}

					if ackMessage.HBEatSeqNum == seqNum {
						<-time.After(time.Until(deadline))
						break InnerLoop
					} else {
						continue InnerLoop
					}
				}
			}
		}

		seqNum++

		if numLost >= arg.LostMsgThresh {
			notifyCh <- FailureDetected{
				UDPIpPort: server.Addr,
				ServerId:  server.ServerId,
				Timestamp: time.Now(),
			}

			<-stopHBeat
			stopHBeatMap.Delete(server.ServerId)
			return
		}
	}
}

func ack(conn *net.UDPConn) {
	defer conn.Close()

	var rs readStruct
	rs.recvBuf = make([]byte, 1024)

	for {
		select {
		case <-stopAck:
			return
		case <-handleRead(&rs, time.Time{}, conn):
			if rs.err != nil {
				continue
			}

			time.Sleep(1 * time.Second)

			hbeatMessage, _ := decodeHBeatMessage(rs.recvBuf, rs.len)

			ackMessage := AckMessage{
				HBEatEpochNonce: hbeatMessage.EpochNonce,
				HBEatSeqNum:     hbeatMessage.SeqNum,
			}

			conn.WriteTo(encodeAckMessage(&ackMessage), rs.from)
		}
	}
}

func encodeAckMessage(msg *AckMessage) []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(msg)
	return buf.Bytes()
}

func encodeHBeatMessage(msg *HBeatMessage) []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(msg)
	return buf.Bytes()
}

func decodeAckMessage(buf []byte, len int) (AckMessage, error) {
	var decoded AckMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decoded)

	if err != nil {
		return AckMessage{}, err
	}

	return decoded, nil
}

func decodeHBeatMessage(buf []byte, len int) (HBeatMessage, error) {
	var decoded HBeatMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decoded)

	if err != nil {
		return HBeatMessage{}, err
	}

	return decoded, nil
}

func handleRead(rs *readStruct, deadline time.Time, conn *net.UDPConn) <-chan bool {
	readDone := make(chan bool)

	go func() {
		conn.SetReadDeadline(deadline)
		rs.len, rs.from, rs.err = conn.ReadFrom(rs.recvBuf)

		readDone <- true
	}()

	return readDone
}
