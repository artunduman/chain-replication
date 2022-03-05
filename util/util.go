package util

import (
	"net"
	"net/rpc"
	"strconv"
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

func Max(x, y uint64) uint64 {
	if x < y {
		return y
	}

	return x
}

func SplitAndGetRPCClient(localAddr string, remoteAddr string) (*rpc.Client, error) {
	localIp, _, err := net.SplitHostPort(localAddr)

	if err != nil {
		return nil, err
	}

	port, err := GetFreeTCPPort(localIp)

	if err != nil {
		return nil, err
	}

	client, err := GetRPCClient(
		net.JoinHostPort(localIp, strconv.Itoa(port)),
		remoteAddr,
	)

	if err != nil {
		return nil, err
	}

	return client, nil
}
