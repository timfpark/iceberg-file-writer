package main

type StorageAdapter interface {
	Start() (err error)
	Stop() (err error)
}
