package Io

import (
	"github.com/maxim-kuderko/coeus/events"
)

type Input func() chan *events.Events
type Output func(chan *events.Events)
