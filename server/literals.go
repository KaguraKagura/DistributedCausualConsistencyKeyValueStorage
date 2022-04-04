package server

import (
	"fmt"
	"strings"

	"Lab2/util"
)

const (
	startCmd = "start"
	hCmd     = "h"
	helpCmd  = "help"
	qCmd     = "q"
	quitCmd  = "quit"

	badArguments                     = "bad arguments"
	goodbye                          = "goodbye"
	unrecognizedCommand              = "unrecognized command"
)

var helpMessage = strings.Join([]string{
	fmt.Sprintf("\t%s [ip:port to listen to] [ip:port of other servers (if multiple, separate by space)]", startCmd),
	fmt.Sprintf("\t%s, %s", quitCmd, qCmd),
	fmt.Sprintf("\t%s, %s", helpCmd, hCmd),
}, "\n")

var helpPrompt = fmt.Sprintf("Type %q or %q to see command usages", helpCmd, hCmd)
var welcomeMessage = fmt.Sprintf("Welcome to %q. You are running this app as a server.\n%s", util.AppName, helpPrompt)
