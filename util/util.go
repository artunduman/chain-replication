package util

import "net"

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
