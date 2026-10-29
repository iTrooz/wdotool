# Persistent input server

Launch one foreground server, then opt individual CLI calls into it:

```sh
wdotool --backend libei server launch
# In another terminal:
wdotool --remote key ctrl+c
wdotool --remote mousemove 100 200
wdotool server status
wdotool server stop
```

`ready` on the launcher's stdout means backend initialization finished.
On KDE, `--backend kde` also provides the existing KWin window operations;
`--backend libei` supplies only libei's capabilities. Without `--backend`,
launch selects the detector's first candidate once, with no fallback loop.
The backend and RemoteDesktop/EIS session remain owned by the server.
There is no daemonization, implicit startup, direct-mode fallback, command
retry, or automatic reconnection. Restart explicitly after authorization loss.

Without `--remote`, commands retain their existing direct behavior. `prime`
remains the separate wlroots device keeper. Help and `diag` stay local and do
not create a portal session. Remote `record` and `prime` are rejected;
`type --file` and `replay` load files or `-` stdin in the client before sending.
Recording continues to use its existing independent capture lifecycle.

## Socket and protocol

The default is `$XDG_RUNTIME_DIR/wdotool/control.sock`. Use `--socket PATH` on
both server management and remote calls to override it. The immediate parent
must already be a user-owned, non-symlink directory with mode `0700` (the
default `wdotool` subdirectory is created automatically). Socket mode is `0600`;
both peers verify the kernel-reported UID. This protects against other users,
not hostile processes running as the same user.

A persistent `.lock` file serializes startup. Only a user-owned Unix socket
whose connection is refused is considered stale. Files and symlinks are never
removed; cleanup checks the socket's device/inode before unlinking it.
Do not remove a running server's lock file.

Protocol version 1 uses length-prefixed JSON, one request per connection,
with a 16 MiB frame limit. Commands execute serially in queue-admission order;
concurrent clients have no ordering guarantee before admission. The queue
holds 32 commands; framing/management connections are limited to 64.
Output is buffered (not streamed), limited to 1 MiB each for stdout/stderr,
and forwarded separately with the command's exit status. Frame reads and
response writes have five-second limits; execution has no overall timeout.
Status and stop remain available during active commands; during initialization,
status reports `starting` and stop cancels startup. A partial startup frame can
delay initialization polling by up to its framing timeout.

## Input and shutdown

Successful `keydown`/`mousedown` holds persist across remote calls. Release
them with `keyup`/`mouseup`, or stop the server. A failed command, active-client
EOF, unexpected extra request data, or undeliverable response conservatively
releases all server-held input, including holds from earlier clients.
Normal EOF after a successfully delivered response does not undo explicit holds.
Queued clients that disconnect are skipped before dispatch.

Stop, Ctrl-C and SIGTERM cancel active work and release held input before
closing the backend. Queued work is rejected. Libei tracks emitted raw keys
and buttons, including implicit Shift. Portal closure, EIS disconnect,
selected-device pause/removal, and emission flush failure are terminal:
the server stops rather than reconnecting. After revocation, explicit releases
may no longer be deliverable; EIS/session teardown removes the devices.
Cleanup errors return failure, including to a stop client. Portal close attempts
are bounded to three seconds; interrupted startup uses best-effort cleanup.
SIGKILL/process crashes cannot run explicit cleanup and rely on compositor
connection teardown. A transport failure may follow partial execution; clients
must not blindly retry.

Portal consent, the existing restore-token policy, revocation, and KDE's
remote-control indicator are unchanged. The indicator can remain visible for
the server's entire authorized lifetime; this mode does not hide it.

## Verification

`cli_server` and `server_transport` exercise real Unix sockets and separate
client subprocesses against an offline controllable backend. They cover
reuse/routing, output/status, serialization, cancellation, explicit holds,
revocation, signal-future shutdown, client file/stdin loading, stale sockets,
unsafe paths, and malformed/versioned frames. Existing mock-backed CLI and
core unit suites pass; default and no-default-feature builds are checked.

No live KDE authorization or input tests have been performed. Actual portal
reuse, visible indicators, delivered key/button releases, and compositor-side
revocation/teardown still require a consented live acceptance test. Offline
mock lifecycle tests do not prove those compositor behaviors.
