package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
)

func main() {
	input := flag.String("input", "", "input path")
	output := flag.String("output", "", "output file path")
	flag.Parse()
	fmt.Println("読み込み:", *input)
	fmt.Println("書き込み:", *output)
	fmt.Println("ファイル結合処理を開始します")
	if isExists(*output) {
		fmt.Println("同一ファイルが存在したので削除します")
		if err := os.Remove(*output); err != nil {
			panic(err)
		}
	}
	ctx := context.Background()
	err := run(ctx, *input, *output)
	if err != nil {
		panic(err)
	}
	fmt.Print("done!\n\n")
}

func isExists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}

func run(ctx context.Context, input, output string) error {
	pr, pw := io.Pipe()
	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer pw.Close()
		return readFiles(ectx, input, pw)
	})
	eg.Go(func() error {
		defer pr.Close()
		return write(ectx, output, pr)
	})
	return eg.Wait()
}

func readFiles(ctx context.Context, input string, writer *io.PipeWriter) error {
	var header []byte
	var total int
	fmt.Print("reading... ")
	defer fmt.Println("")
	err := filepath.Walk(input, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		total++
		fmt.Printf("%d,", total)
		scanner := bufio.NewScanner(file)
		if scanner.Scan() {
			h := scanner.Bytes()
			if bytes.Compare(h, header) != 0 {
				if _, err := file.Seek(0, 0); err != nil {
					return err
				}
				if len(header) == 0 {
					header = h
				}
			} else {
				buff := make([]byte, 2)
				n, err := file.ReadAt(buff, int64(len(h)))
				if err != nil {
					return err
				}
				var skipLen int64
				const max = 2
				if n >= max {
					skipLen = countCRLF(buff[:max])
				}
				if _, err := file.Seek(int64(len(h))+skipLen, 0); err != nil {
					return err
				}
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		_, err = io.Copy(writer, file)
		return err
	})
	return err
}

func write(ctx context.Context, output string, reader *io.PipeReader) error {
	file, err := os.Create(output)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, reader)
	return err
}

func countCRLF(bs []byte) int64 {
	const CR, LF = 13, 10
	var total int64
	for _, b := range bs {
		switch b {
		case CR, LF:
			total++
		default:
			return total
		}
	}
	return total
}
