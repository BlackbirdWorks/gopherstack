package gcpenforce_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
)

func TestStreamHandlerPanicReturnsInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want codes.Code
	}{
		{name: "panic", want: codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lis := bufconn.Listen(1 << 16)
			srv := grpc.NewServer(append(gcpenforce.RecoverOptions(),
				grpc.UnknownServiceHandler(func(any, grpc.ServerStream) error { panic("injected rpc panic") }))...)

			go func() { _ = srv.Serve(lis) }()

			t.Cleanup(srv.Stop)

			dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
			cc, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(dialer),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = cc.Close() })

			for range 2 {
				st, serr := cc.NewStream(t.Context(), &grpc.StreamDesc{}, "/x.Y/Z")
				require.NoError(t, serr)
				require.NoError(t, st.SendMsg(&emptypb.Empty{}))
				require.NoError(t, st.CloseSend())

				rerr := st.RecvMsg(&emptypb.Empty{})
				assert.Equal(t, tt.want, status.Code(rerr))
			}
		})
	}
}
