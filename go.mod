module github.com/arcology-network/scheduler

go 1.22

replace github.com/ethereum/go-ethereum v1.14.8 => ../concurrent-evm/

replace github.com/arcology-network/common-lib => ../common-lib/

replace github.com/arcology-network/storage-committer => ../storage-committer/

require (
	github.com/holiman/uint256 v1.2.4
	golang.org/x/exp v0.0.0-20231206192017-f3f8817b8deb
)
