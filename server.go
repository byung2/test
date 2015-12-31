package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	//"io"
	"github.com/pquerna/ffjson/ffjson"
	"net"
	"runtime"
	"strings"
)

type BridgeMsg struct {
	Version string `json:"version"`
	Type    string `json:"type"`
	Level   string `json:"level"`
	State   string `json:"state"`
	Time    int64  `json:"time"`
	Text    string `json:"text"`
	Role    string `json:"role"`
	Host    string `json:"host"`
	From    string `json:"from"`
	Uid     string `json:"uid"`
}

type Publisher interface {
	Init() error
	Send(bridgeMsg *BridgeMsg)
}

func handleRequest(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	//var producer Producer
	//producer.Init()

	//var publisher Publisher
	//publisher = producer

	producer := KafkaPublisher{}
	publisher := Publisher(&producer)

	err := publisher.Init()
	if err != nil {
		fmt.Println("failed to init KafkaPublisher", err)
		return
	}

	for {

		if ok := scanner.Scan(); !ok {
			fmt.Println("closed?")
			break
		}

		/*
			// for perf test
				line, rberr := bufio.NewReader(conn).ReadBytes('\n')
				if rberr != nil {
					if rberr == io.EOF {
						fmt.Println("wrong msg", rberr)
						break
					}
					fmt.Println("client seems closed")
					break
				}*/
		//fmt.Println(string(line))

		bridgeMsg := &BridgeMsg{}
		err := bridgeMsg.BytesToBridgeMsg(scanner.Bytes())
		if err != nil {
			//TODO
			fmt.Println("msgErr", err)
			continue
		}
		isNormal := bridgeMsg.HandleMsg()
		if !isNormal {
			continue
		}
		/*
			str := bridgeMsg.toJsonString()
			if str == "" {

			} else {
				fmt.Println("str", str)
			}
		*/
		producer.Send(bridgeMsg)
		/*
			var bridgeMsg map[string]interface{}
			bridgeMsg = BytesToMap(scanner.Bytes())
			if bridgeMsg != nil {
			}*/

		/*
			isNormal := bridgeMsg.HandleMsg()
			if !isNormal {
				continue
			}
			producer.Send(bridgeMsg)
		*/

	}
	/*
		for {
			line, err := bufio.NewReader(conn).ReadString('\n')

			if err != nil {
				if err == io.EOF {
					fmt.Println("wrong msg")
					return
				}
				fmt.Println("client seems closed")
				return
			}
			//msg := string(line)
			fmt.Println("line", string(line))
			if len(line) > 0 {
				//line = bytes.Trim(line, "\x00")
				//line = line[0 : len(line)-1]
				//fmt.Println(line)
				//bridgeMsg := BytesToBridgeMsg(line)
				//if bridgeMsg.
				//fmt.Println("line:", bridgeMsg)
			}

		}
	*/
}

func BytesToMap(msg []byte) map[string]interface{} {
	var msgMap map[string]interface{}
	err := json.Unmarshal(msg, &msgMap)
	if err != nil {
		//error.New("parsing error TODO")
		return nil
	}
	return msgMap
}

func StringToBridgeMsg(msg string) *BridgeMsg {
	r := strings.NewReader(msg)
	d := json.NewDecoder(r)
	m := BridgeMsg{}

	fmt.Println(d.Decode(&m))
	return &m
}

func (msg *BridgeMsg) toJsonString() string {
	//bmsgB, err := ffjson.Marshal(msg)
	bmsgB, err := msg.MarshalJSON()
	if err != nil {
		fmt.Println("error:", err)
		return ""
	}
	str := string(bmsgB)
	ffjson.Pool(bmsgB)
	return str
}

func (bridgeMsg *BridgeMsg) BytesToBridgeMsg(msg []byte) error {
	//err := json.Unmarshal(msg, &bridgeMsg)
	//err := ffjson.Unmarshal(msg, &bridgeMsg)
	err := bridgeMsg.UnmarshalJSON(msg)
	if err != nil {
		fmt.Println(err)
		return errors.New("parsing error TODO")
		//return nil
	}
	return nil
	//return &bridgeMsg
}

func (msg *BridgeMsg) HandleMsg() bool {
	if msg.Type != "metric" {
		state := msg.State
		if state == "" {
			return false
		}
		// lowercase state
		// compare log-level with config value
		// re-assign to orig-value with lowercase value (msg.State=state)
		if len(msg.Role) == 0 {
			msg.Role = "TODO-ROLE"
		}
		if len(msg.Host) == 0 {
			msg.Role = "TODO-HOST"
		}
		// farmName + ?
		if len(msg.From) == 0 {
			msg.From = "TODO-From"
		}
		if len(msg.Uid) == 0 {
			msg.Uid = "TODO-Uid"
		}
	}
	return true
}

func main() {
	runtime.GOMAXPROCS(2)
	psock, err := net.Listen("tcp", ":7299")
	if err != nil {
		return
	}

	for {
		conn, err := psock.Accept()
		if err != nil {
			return
		}

		//channel := make(chan string)
		fmt.Println("handleReq")

		go handleRequest(conn)
	}
}
