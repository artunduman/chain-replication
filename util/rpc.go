package util

import (
	"net"
	"net/rpc"
)

func GetRPCClient(localAddr string, remoteAddr string) (*rpc.Client, error) {
	laddr, err := net.ResolveTCPAddr("tcp", localAddr)
	raddr, err := net.ResolveTCPAddr("tcp", remoteAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", laddr, raddr)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}
