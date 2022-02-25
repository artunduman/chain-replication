package util

import (
	"log"
	"net"
	"net/rpc"
)

const (
	MAX_SERVER_COUNT = 16
)

func GetPreferredOutboundIp() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

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

func RemoveUInt32(s []uint32, num uint32) []uint32 {
	start := 0
	end := len(s) - 1
	index := binarySearch(s, num, start, end)
	if index != -1 {
		return append(s[:index], s[index+1:]...)
	} else {
		return s
	}
}

func binarySearch(s []uint32, num uint32, start int, end int) int {
	if start > end {
		return -1
	}

	mid := (start + end) / 2
	if s[mid] == num {
		return mid
	} else if s[mid] > num {
		return binarySearch(s, num, start, mid-1)
	} else {
		return binarySearch(s, num, mid+1, end)
	}
}
