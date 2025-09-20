package raftconfig

import "github.com/cockroachdb/cockroach/pkg/util/envutil"

// AUTO_DECIDING_MESSAGE_SENDING is initialized once from the environment.
// Set COCKROACH_AUTO_DECIDING_MESSAGE_SENDING=true to enable.
var AUTO_DECIDING_MESSAGE_SENDING = envutil.EnvOrDefaultBool(
	"COCKROACH_AUTO_DECIDING_MESSAGE_SENDING", false,
)
