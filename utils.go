package utils

import (
	"os"
	"strings"
)

func DoesDirectoryExist(path string) (bool, string) {
	lastIndexSlash := strings.LastIndex(path, "/")
	directoryPath := path[:lastIndexSlash]
	_, err := os.Stat(directoryPath)

	return !os.IsNotExist(err), directoryPath
}

// Find takes a slice and looks for an element in it. If found it will
// return it's key, otherwise it will return -1 and a bool of false.
func Contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func Keys(m map[string]string) []string {
	keys := make([]string, len(m))

	i := 0
	for k := range m {
		keys[i] = k
		i++
	}

	return keys
}

func DirPart(path string) string {
	return path[:strings.LastIndex(path, "/")]
}

func NamePart(path string) string {
	return path[strings.LastIndex(path, "/")+1:]
}
