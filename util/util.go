package util

import (
	"net"
	"net/rpc"
)

func GetFreeTCPPort(addrIp string) (int, error) {
	ipPort, err := net.ResolveTCPAddr(
		"tcp",
		net.JoinHostPort(addrIp, "0"),
	)

	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", ipPort)

	if err != nil {
		return 0, err
	}

	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func GetFreeUDPPort(addrIp string) (int, error) {
	ipPort, err := net.ResolveUDPAddr(
		"udp",
		net.JoinHostPort(addrIp, "0"),
	)

	if err != nil {
		return 0, err
	}

	l, err := net.ListenUDP("udp", ipPort)

	if err != nil {
		return 0, err
	}

	defer l.Close()
	return l.LocalAddr().(*net.UDPAddr).Port, nil
}

func GetRPCClient(localAddr string, remoteAddr string) (*rpc.Client, error) {
	laddr, err := net.ResolveTCPAddr("tcp", localAddr)

	if err != nil {
		return nil, err
	}

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
