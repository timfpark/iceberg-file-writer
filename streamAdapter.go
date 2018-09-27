package main

type StreamAdapter interface {
	Start() (err error)
	Stop() (err error)
}
