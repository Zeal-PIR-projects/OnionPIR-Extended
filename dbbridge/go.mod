module github.com/Zeal-PIR-projects/OnionPIR-Extended/dbbridge

go 1.25.5

require (
	github.com/Zeal-PIR-projects/shuffle v0.0.0
	google.golang.org/grpc v1.71.1
)

require (
	github.com/tink-crypto/tink-go/v2 v2.6.0 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/protobuf v1.36.8 // indirect
)

// The database API (proto_gen) and record format (utility) come straight from
// the Zealot/shuffle checkout, so this server speaks exactly the same protocol
// as Zealot's database streamer. CMake builds from a copy of this file whose
// replace points at -DSHUFFLE_DIR (default: ../shuffle next to this repo).
replace github.com/Zeal-PIR-projects/shuffle => ../../shuffle
