#!/bin/sh

SOURCE=github.com/suzujun/filejoiner
PREFIX=filejoiner
VERSION=v0.0.1

rm -f release/*
mkdir -p release

GOOS=darwin  GOARCH=amd64 go build -o release/${PREFIX}_darwin_amd64_${VERSION} ${SOURCE}
GOOS=darwin  GOARCH=arm64 go build -o release/${PREFIX}_darwin_arm64_${VERSION} ${SOURCE}
GOOS=linux   GOARCH=386   go build -o release/${PREFIX}_linux_386_${VERSION} ${SOURCE}
GOOS=linux   GOARCH=amd64 go build -o release/${PREFIX}_linux_amd64_${VERSION} ${SOURCE}
GOOS=linux   GOARCH=arm64 go build -o release/${PREFIX}_linux_arm64_${VERSION} ${SOURCE}
GOOS=windows GOARCH=386   go build -o release/${PREFIX}_windows_386_${VERSION}.exe ${SOURCE}
GOOS=windows GOARCH=amd64 go build -o release/${PREFIX}_windows_amd64_${VERSION}.exe ${SOURCE}
GOOS=windows GOARCH=arm64 go build -o release/${PREFIX}_windows_arm64_${VERSION}.exe ${SOURCE}
GOOS=windows GOARCH=arm   go build -o release/${PREFIX}_windows_arm_${VERSION}.exe ${SOURCE}
