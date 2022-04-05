package client

import (
	"fmt"
	"strings"

	"Lab2/util"
)

const (
	connectCmd = "connect"
	readCmd    = "read"
	writeCmd   = "write"
	hCmd       = "h"
	helpCmd    = "help"
	qCmd       = "q"
	quitCmd    = "quit"

	badArguments        = "bad arguments"
	goodbye             = "goodbye"
	unrecognizedCommand = "unrecognized command"
)

var helpMessage = strings.Join([]string{
	fmt.Sprintf("\t%s [ip:port of server]", connectCmd),
	fmt.Sprintf("\t%s [key]", readCmd),
	fmt.Sprintf("\t%s [key] [value]", writeCmd),
	fmt.Sprintf("\t%s, %s", helpCmd, hCmd),
	fmt.Sprintf("\t%s, %s", quitCmd, qCmd),
}, "\n")

var helpPrompt = fmt.Sprintf("Type %q or %q to see command usages", helpCmd, hCmd)
var welcomeMessage = fmt.Sprintf("Welcome to %q. You are running this app as a client.\n%s", util.AppName, helpPrompt)
