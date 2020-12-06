package httpsvr

import (
	"net"
	"testing"
)

func TestInRange(t *testing.T) {

	expect := func(expected bool, ipAddress string) {
		actual := isPrivateSubnet(net.ParseIP(ipAddress))
		if actual != expected {
			t.Errorf(
				"ipAddress %v expected %t, got %t",
				ipAddress, expected, actual,
			)
		}

	}
	/*
		start: net.ParseIP("10.0.0.0"),
		end:   net.ParseIP("10.255.255.255"),
	*/
	expect(true, "10.0.0.0")
	expect(true, "10.255.255.254")
	expect(true, "10.0.0.1")
	expect(true, "10.0.0.255")
	expect(true, "10.0.1.0")
	expect(true, "10.0.255.0")
	expect(true, "10.1.0.0")
	expect(true, "10.255.0.0")
	expect(true, "10.1.1.1")
	expect(true, "10.1.1.1")
	expect(false, "10.255.255.255")
	expect(false, "10.0.0.256")
	expect(false, "10.256.0.255")
	expect(false, "11.0.0.255")
	expect(false, "11.0.0.0")
	expect(false, "9.0.0.0")
	expect(false, "10.-1.0.0")
	expect(false, "10.0.-1.0")
	expect(false, "10.0.0.-1")

	/*
		start: net.ParseIP("100.64.0.0"),
		end:   net.ParseIP("100.127.255.255"),
	*/
	expect(true, "100.64.0.0")
	expect(true, "100.127.255.254")
	expect(true, "100.127.255.1")
	expect(true, "100.127.1.255")
	expect(true, "100.64.255.255")
	expect(true, "100.127.0.0")
	expect(true, "100.127.255.0")
	expect(true, "100.127.0.255")
	expect(false, "100.127.255.255")
	expect(false, "100.63.0.0")
	expect(false, "99.64.0.0")
	expect(false, "100.63.0.0")
	expect(false, "100.128.0.0")
	expect(false, "100.127.0.256")
	expect(false, "100.127.256.0")
	expect(false, "100.127.-1.0")
	expect(false, "100.127.0.-1")

	/*
		start: net.ParseIP("172.16.0.0"),
		end:   net.ParseIP("172.31.255.255"),
	*/
	expect(true, "172.16.0.0")
	expect(true, "172.31.255.254")
	expect(true, "172.16.255.1")
	expect(true, "172.16.0.255")
	expect(true, "172.31.255.0")
	expect(true, "172.16.0.0")
	expect(true, "172.16.255.0")
	expect(true, "172.16.0.255")
	expect(false, "172.31.255.255")
	expect(false, "172.15.0.0")
	expect(false, "172.32.0.0")
	expect(false, "171.31.0.0")
	expect(false, "171.32.0.0")
	expect(false, "172.16.0.256")
	expect(false, "172.31.256.0")
	expect(false, "172.31.-1.0")
	expect(false, "172.16.0.-1")

	/*
		start: net.ParseIP("192.0.0.0"),
		end:   net.ParseIP("192.0.0.255"),
	*/
	expect(true, "192.0.0.0")
	expect(true, "192.0.0.254")
	expect(false, "192.0.1.0")
	expect(false, "192.0.0.255")
	expect(false, "192.0.1.1")
	expect(false, "192.1.0.1")
	expect(false, "192.0.1.0")
	expect(false, "192.0.0.-1")
	expect(false, "192.0.0.256")
	/*
		start: net.ParseIP("192.168.0.0"),
		end:   net.ParseIP("192.168.255.255"),
	*/
	expect(true, "192.168.0.0")
	expect(true, "192.168.255.254")
	expect(true, "192.168.0.1")
	expect(true, "192.168.1.255")
	expect(true, "192.168.255.254")
	expect(false, "192.168.255.-1")
	expect(false, "192.168.256.0")

	/*
		start: net.ParseIP("198.18.0.0"),
		end:   net.ParseIP("198.19.255.255"),
	*/
	expect(true, "198.18.0.0")
	expect(true, "198.19.255.254")
	expect(true, "198.18.0.1")
	expect(true, "198.18.0.255")
	expect(true, "198.18.1.0")
	expect(true, "198.18.1.254")
	expect(true, "198.18.255.0")
	expect(true, "198.18.255.254")
	expect(true, "198.19.0.0")
	expect(true, "198.19.0.1")
	expect(true, "198.19.0.254")
	expect(true, "198.19.1.0")
	expect(true, "198.19.1.1")
	expect(true, "198.19.1.255")
	expect(true, "198.19.255.0")
	expect(true, "198.19.255.254")
	expect(false, "198.17.255.254")
	expect(false, "198.20.0.0")
	expect(false, "198.20.0.1")
	expect(false, "199.18.0.1")
	expect(false, "197.18.0.1")
	expect(false, "198.18.0.-1")
	expect(false, "198.18.0.256")
	expect(false, "198.18.-1.0")
	expect(false, "198.18.256.0")
}
