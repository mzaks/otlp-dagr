// Command otapsend streams OTAP batches (serialized BatchArrowRecords — the Dagr -> OTAP
// bridge's .bar / .sbar / .spbar files) to an OpenTelemetry Arrow receiver over ONE gRPC
// stream, in the order given, and requires an OK BatchStatus for every one.
//
//	otapsend -endpoint 127.0.0.1:4317 -signal traces a.sbar b.sbar …
//
// One invocation is one stream: files a producer wrote on one stream (schemas and
// dictionaries carried across) must be sent together, in order.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	arrowpb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type stream interface {
	Send(*arrowpb.BatchArrowRecords) error
	Recv() (*arrowpb.BatchStatus, error)
	CloseSend() error
}

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:4317", "receiver gRPC endpoint")
	signal := flag.String("signal", "traces", "traces | logs | metrics")
	flag.Parse()
	if err := send(*endpoint, *signal, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "otapsend:", err)
		os.Exit(1)
	}
}

func send(endpoint, signal string, files []string) error {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var s stream
	switch signal {
	case "traces":
		s, err = arrowpb.NewArrowTracesServiceClient(conn).ArrowTraces(ctx)
	case "logs":
		s, err = arrowpb.NewArrowLogsServiceClient(conn).ArrowLogs(ctx)
	case "metrics":
		s, err = arrowpb.NewArrowMetricsServiceClient(conn).ArrowMetrics(ctx)
	default:
		return fmt.Errorf("unknown signal %q", signal)
	}
	if err != nil {
		return err
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		var bar arrowpb.BatchArrowRecords
		if err := proto.Unmarshal(raw, &bar); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		if err := s.Send(&bar); err != nil {
			return fmt.Errorf("%s: send: %w", f, err)
		}
		// one status per batch, in order: wait for it, so the collector has consumed this
		// batch before the next is sent and the exporter's lines come out in file order
		st, err := s.Recv()
		if err != nil {
			return fmt.Errorf("%s: status: %w", f, err)
		}
		if st.GetBatchId() != bar.GetBatchId() || st.GetStatusCode() != arrowpb.StatusCode_OK {
			return fmt.Errorf("%s: batch %d answered %d %s: %s", f, bar.GetBatchId(), st.GetBatchId(),
				st.GetStatusCode(), st.GetStatusMessage())
		}
	}
	return s.CloseSend()
}
