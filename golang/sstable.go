package main

import (
	"os"
)

type SSTable struct {
	filename string
	file     *os.File
}
