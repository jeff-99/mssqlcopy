package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func input(question string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(question)
	text, _ := reader.ReadString('\n')
	return strings.Trim(text, "\n")
}
