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
	UDPIpPort string    // The RemoteIP:RemotePort of the failed node.
	Timestamp time.Time // The time when the failure was detected.
}

////////////////////////////////////////////////////// API

type StartStruct struct {
	AckLocalIPAckLocalPort       string
	EpochNonce                   uint64
	HBeatLocalIPHBeatLocalPort   string
	HBeatRemoteIPHBeatRemotePort string
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
var stopHBeat chan bool = make(chan bool)

// Starts the fcheck library.
func Start(arg StartStruct) (notifyCh <-chan FailureDetected, err error) {
	if isActive {
		return nil, errors.New("fcheck is already monitoring")
	}

	if arg.HBeatLocalIPHBeatLocalPort == "" {
		err := ackInit(arg)

		if err != nil {
			return nil, err
		}

		isActive = true

		return nil, nil
	}

	err = ackInit(arg)

	if err != nil {
		return nil, err
	}

	notifyCh, err = hbeatInit(arg)

	if err != nil {
		stopAck <- true
		return nil, err
	}

	isActive = true

	return notifyCh, nil
}

// Tells the library to stop monitoring/responding acks.
func Stop() {
	if isActive {
		stopAck <- true
		stopHBeat <- true

		isActive = false
	}
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

func hbeatInit(arg StartStruct) (<-chan FailureDetected, error) {
	laddr, err := net.ResolveUDPAddr("udp", arg.HBeatLocalIPHBeatLocalPort)

	if err != nil {
		return nil, errors.New("failed to resolve UDP address")
	}

	raddr, err := net.ResolveUDPAddr("udp", arg.HBeatRemoteIPHBeatRemotePort)

	if err != nil {
		return nil, errors.New("failed to resolve UDP address")
	}

	conn, err := net.DialUDP("udp", laddr, raddr)

	if err != nil {
		return nil, errors.New("failed to dial")
	}

	notifyCh := make(chan FailureDetected, 2)

	go hbeat(arg, notifyCh, conn)

	return notifyCh, nil
}

func hbeat(arg StartStruct, notifyCh chan FailureDetected, conn *net.UDPConn) {
	defer conn.Close()

	seqNumMap := make(map[uint64]time.Time)

	var rs readStruct
	rs.recvBuf = make([]byte, 1024)

	seqNum := uint64(0)
	rtt := time.Duration(3) * time.Second
	numLost := uint8(0)

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
				close(notifyCh)
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
				UDPIpPort: arg.HBeatRemoteIPHBeatRemotePort,
				Timestamp: time.Now(),
			}

			close(notifyCh)
			<-stopHBeat
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
