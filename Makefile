IPFS_PATH ?= ${HOME}/.ipfs

gx:
	go get github.com/whyrusleeping/gx
	go get github.com/whyrusleeping/gx-go

deps: gx
	gx --verbose install --global
	gx-go rewrite

build: deps
	go build -buildmode=plugin -o=oss_plugin.so ./plugin

install: build
	mkdir -p ${IPFS_PATH}/plugins
	chmod +x oss_plugin.so
	rm -f ${IPFS_PATH}/plugins/oss_plugin.so
	cp oss_plugin.so ${IPFS_PATH}/plugins/
