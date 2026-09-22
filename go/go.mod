module otlpdagr

go 1.26.0

replace otlptraces => ./gen

require (
	go.opentelemetry.io/collector/pdata v1.67.0
	otlptraces v0.0.0-00010101000000-000000000000
)

require (
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	go.opentelemetry.io/collector/featuregate v1.67.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
)
