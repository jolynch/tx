# FTCP Authentication Specification (noise-v1)

This document defines how an FTCP connection authenticates and encrypts its
traffic: server key discovery, the Noise handshake that starts a session, and
the record layer that carries commands and responses afterwards.

A connection either stays plaintext for its whole life or authenticates on its
first line. An authenticated connection keeps one Noise session for its whole
life: every command and response on it, including keep-alive heartbeats, is
encrypted under that session's keys, and pooled reuse performs no further key
exchange.

The implementation lives in `internal/aead` (handshake, records, credentials),
`internal/filexfer/ftcp` (server), and `client_auth.go` (client). The
[CLI reference](../pub/CLI.md#authentication-and-encryption) covers the
corresponding flags.

## Server Policy

Every server accepts `AUTH`. Its configuration decides whether authentication
is required and which credentials it accepts:

| Configuration                 | Plaintext clients | Authenticated clients                                                                      |
|-------------------------------|-------------------|--------------------------------------------------------------------------------------------|
| No flags                      | Accepted          | Age mode with any client key, after proof of its private key                               |
| `--require-auth`              | Rejected          | Age mode with any client key, after proof of its private key                               |
| One or more allowlist entries | Rejected          | Token mode with an allowed token, or age mode with the private key of an allowed recipient |

The CLI's allowlist flags are `--require-auth-token[-file]` for tokens and
`--require-client-recipient[-file]` for client age recipients. In the
library, `ServerOptions.RequireAuth` selects the second row and any
`ServerOptions.AllowedAuthTokens` or `AllowedClientRecipients` entry selects
the third, whatever `RequireAuth` says. On a server that requires
authentication, the first line may be at most 256 bytes: a longer one gets
`ERR BAD_REQUEST command line too large`, and a shorter plaintext command gets
`ERR NOT_AUTHORIZED missing AUTH`.

## Credentials

A client authenticates in one of two modes:

- **Token mode**: the client proves possession of a secret token that the
  server allowlists, and of a client key pair that the server does not check
  against any list. The token alone authorizes the connection.
- **Age mode**: the client proves possession of the private key for an age
  X25519 recipient. On a server with an allowlist the recipient must be listed;
  otherwise any key passes.

In both modes the client proves its static private key and the server proves
the private key of the key it advertised in discovery.

### Tokens

A token is 16–4096 bytes, each a visible ASCII character (33–126). Tokens are
opaque and compared byte for byte; they are never trimmed. A token must not
begin with `age1` or `AGE-SECRET-KEY-` in any case, so a mistyped recipient or a
pasted private key can never become a token. Generated tokens are 32 random
bytes as 64 hexadecimal characters.

Nothing on the wire lets a passive observer test token guesses (see
[Credential tag and PSK](#credential-tag-and-psk)), so a token only has to
resist online guessing and an active attacker who intercepts a client that
does not require the server's recipient. Length checks cannot establish
entropy: a token chosen by hand must encode at least 64 random bits, such as
16 random hexadecimal characters, and a client using one should require the
server's recipient.

### Age keys

Client and server keys are age X25519 identities and recipients, validated with
age's parsers. age exposes no raw key bytes, so tx extracts the 32-byte key
from the string age has accepted; it never encodes keys. Public keys must be
canonical: bit 255 clear and below 2^255−19. X25519 ignores bit 255 and reduces
the u-coordinate modulo p, so a non-canonical key would give one key several
recipient strings and credential tags. Age recipients are public and may be
published.

### Allowlist entries

The server allowlist holds two typed lists: secret tokens and client age
recipients. Each entry's type comes from its list, not its contents. A token
must be a valid token, so a recipient or private key in the token list fails
startup; a client recipient must parse as a canonical recipient, so a private
key or anything else fails startup. A recipient never matches a token-mode
handshake, even though its string would pass as a token's bytes.

### Credential tag and PSK

```text
es  = X25519(client ephemeral private key, server static public key)
tag = lowercase-hex(HMAC-SHA256(key = es,
          "tx/ftcp/noise-v1/id/" || mode || 0x00 || credential))
PSK = SHA256("tx/ftcp/noise-v1/psk/token" || 0x00 || token)
```

`mode` is `token` or `age`. `credential` is the token's bytes or, in age mode,
the client's raw 32-byte public key. The tag selects an allowlist entry; it
never authorizes anything. `es` is the same value Noise computes for message
1's `es` token, so only the client and the server can compute the tag. A
passive observer can neither test token guesses against it nor link a
client's connections, because every connection uses a fresh ephemeral key. The
PSK is credential-equivalent and exists only in token mode.

The server also names each credential by a stable fingerprint,
`lowercase-hex(SHA256("tx/ftcp/noise-v1/id/" || mode || 0x00 || credential))`.
It deduplicates allowlist entries and identifies an authenticated client, and
it is never sent.

## Key Discovery

A client learns the server's public key and cipher hint on a connection of its
own:

```text
client: AUTH key noise-v1\r\n
server: OK <aes|chacha20> <server-age-recipient>\r\n
server closes the connection
```

The discovery line names the protocol version. Clients from before
`noise-v1` send the bare `AUTH key` first; the server answers it with
`ERR UNSUPPORTED_AUTH client predates noise-v1; upgrade tx`, which those
clients print before sending anything else.

The client discovers once per client and caches the result for every pool.
The reply is at most 256 bytes, ends in CRLF, and carries a canonical
recipient. Discovery is unauthenticated: an active attacker can substitute its
own key. A client that requires the server's recipient
(`--require-server-recipient`, `WithRequiredServerRecipient`) compares it with
the discovered key before sending any authentication material.

The cipher hint is the server's preference. A client's `auto` setting selects
AES-GCM only when its own CPU accelerates both AES and GHASH and the hint is
`aes`, and ChaCha20-Poly1305 otherwise. An explicit `aes` or `chacha20` ignores
the hint.

## Handshake

### Preface

The client sends the preface and Noise message 1 in one write:

```text
AUTH noise-v1 <token|age> <aes|chacha20> <credential-tag>\r\n
```

Fields are separated by single spaces, the tag is 64 lowercase hexadecimal
characters, and the line ends in CRLF and is at most 256 bytes. The server
rejects anything else. The client draws its ephemeral key before writing the
preface, since the tag depends on it, and message 1 carries that same key.

### Patterns

| Mode  | Cipher     | Noise protocol                         |
|-------|------------|----------------------------------------|
| Token | `aes`      | `Noise_IKpsk0_25519_AESGCM_SHA256`     |
| Token | `chacha20` | `Noise_IKpsk0_25519_ChaChaPoly_SHA256` |
| Age   | `aes`      | `Noise_IK_25519_AESGCM_SHA256`         |
| Age   | `chacha20` | `Noise_IK_25519_ChaChaPoly_SHA256`     |

```text
Token (IKpsk0)                   Age (IK)
<- s   discovered server key     <- s   discovered server key
...                              ...
-> psk, e, es, s, ss             -> e, es, s, ss
<- e, ee, se                     <- e, ee, se
-> FINISH                        -> FINISH
```

The prologue binds the preface:

```text
"tx/ftcp/noise-v1" || 0x00 || the AUTH line exactly as sent, CRLF included
```

Each handshake message is prefixed by its length as a big-endian uint16.
Payloads are empty, so message 1 is 96 bytes and message 2 is 48 bytes;
any other length closes the connection, without a diagnostic, before the
message is read. Every connection generates fresh ephemeral keys; unlike
static keys, an ephemeral key need not be canonical, and only one that makes
`es` zero is refused. Token mode requires a 32-byte PSK and age mode
none; Noise adds the `psk0` modifier only for a non-empty PSK, so this check
keeps token mode from ever running without the token.

### Server checks

Before reading message 1:

- Token mode on a server without token entries is refused with the
  not-configured diagnostic.
- Age mode is refused on a server with no allowlist that does not accept any
  key. A server built from `ServerOptions` always accepts any key when its
  allowlist is empty, so only a direct `aead.ServerConfig` user meets this.

After reading message 1, whose first 32 bytes are the client's ephemeral key
in the clear:

- The server computes `es`. A low-order ephemeral key, which makes `es` zero,
  is refused.
- In token mode, and in age mode on a server with an allowlist, the tag must
  match an entry of that mode. The server computes the tag of every entry of
  the mode and compares each in constant time, without stopping at a match.

After message 1 decrypts:

- The client's static key must be canonical.
- In age mode, the tag must be the tag of that key, and on a server with an
  allowlist the key must equal the matched entry's key. Both comparisons are
  constant-time.

The server then sends message 2.

### FINISH

The client's first transport record is an empty `FINISH` record, at nonce
zero. The client sends it as soon as message 2 decrypts, without waiting for a
command, so a pooled connection authenticates before its first use. The
server verifies `FINISH` before it records the connection as authenticated or
reads any command. `FINISH` is not acknowledged: a server that rejects it
closes the connection, which the client sees when it next reads.
Message 1 can be replayed and message 2 can be produced by anyone holding the
server's key, but only the client that derived the session keys can produce
`FINISH`.

### Diagnostics

Before message 2, the server may answer with one plaintext line and close:

```text
ERR UNSUPPORTED_AUTH expected noise-v1\r\n
ERR UNSUPPORTED_AUTH client predates noise-v1; upgrade tx\r\n
ERR NOT_AUTHORIZED authorization failed\r\n
ERR NOT_AUTHORIZED token authentication not configured\r\n
ERR BUSY server at handshake capacity\r\n
```

Message 2 always begins with the bytes `00 30`, and no diagnostic begins with
`00`, so the client can tell them apart. A first line with the `AUTH` verb,
in any case, that is neither the exact discovery line nor a valid preface gets
`ERR UNSUPPORTED_AUTH`: the bare legacy `AUTH key` gets the "client predates"
line, and every other form, legacy `AUTH aes|chacha20 <blob>` included, gets
"expected noise-v1". The server logs each legacy form with the client's
address, never its contents, and emits an `auth` event with mode `legacy`. On a
server that requires authentication, a legacy `AUTH aes|chacha20 <blob>` line
is usually longer than the 256-byte cap and gets `ERR BAD_REQUEST command line
too large` without the log line; earlier clients send the short bare
`AUTH key` first, so they still see the "client predates" message.
After writing a
diagnostic, the server half-closes and reads the rest of the client's opening
for at most 200 ms, so the client receives the diagnostic instead of a
connection reset. After message 2, every failure closes the connection without
a response.

Diagnostics are unauthenticated hints. A client never falls back to plaintext
or legacy `AUTH` because of one.

## Records

After `FINISH`, every byte in either direction travels in records:

```text
uint16-BE ciphertext length || kind || AEAD(payload, associated data = length || kind)
```

The 3-byte header travels in the clear and is the AEAD's associated data, so
it is authenticated with the payload. This is TLS 1.2's layout; TLS 1.3 also
authenticates its header but moves the real type into the plaintext to hide
it. tx keeps the kind out of the plaintext because an AEAD seals one
contiguous plaintext: a kind byte inside it would force every write to copy
the payload next to it first. With the kind in the header, a writer seals a
full record straight from its caller's buffer and a reader opens one straight
into its caller's buffer, without copying through the session's own.

The record layer hides content, not traffic shape. Records are not padded,
so lengths are visible, and an empty `END` is recognizable by its 16-byte
ciphertext. The cleartext kind also shows which short records are flushes and
which end a message, as soon as each arrives rather than when traffic changes
direction. Commands do not interleave, so this adds little to what lengths and
turn-taking already reveal. Kinds only frame messages: control traffic, such as
keep-alive heartbeats, is ordinary FTCP commands (`PROBE`) inside the
encrypted payload, so the kind never reveals which command a message carries.

| Kind     | Value | Payload   | Meaning                                       |
|----------|-------|-----------|-----------------------------------------------|
| `DATA`   | 0     | Non-empty | More bytes of the current command or response |
| `END`    | 1     | Optional  | Ends the current command or response          |
| `FINISH` | 2     | Empty     | The client's first record only                |

### Parsing rules

- Ciphertext length is 16–65,535 bytes: the 16-byte tag plus up to 65,519
  payload bytes. A shorter length is rejected before the record is read.
- A reader returns end of message only at an authenticated `END`. Record
  boundaries carry no other meaning: a message may contain short `DATA`
  records anywhere, and a frame may span records.
- `END` may carry payload; a command or response is complete only when its
  grammar has consumed every byte through `END`.
- A clean close before the first record of a message is a disconnect, not a
  message. A server reads it as the client leaving between commands; a client
  waiting for a response treats it as an error, since a response is complete
  only at its terminal status line and `END`. No close record is needed.

### Errors

The connection closes, and its session is unusable afterwards, on:

- a record that fails authentication or has an invalid length
- kinds 3–255, `DATA` with an empty payload, `FINISH` anywhere but the
  client's first record, or `FINISH` from the server
- EOF inside a message, before its `END`
- an interrupted or partial write

A failed session is never resynchronized, and its connection never returns to
a pool.

### Ciphers and nonces

Noise runs only the handshake. The record layer takes the two keys from
`Split` (client-to-server first) and owns the AEAD, the nonce counter, and
rekeying. Unlike a Noise transport message, a record has non-empty
associated data, its header. Nonces and rekeying follow Noise's definitions:

- The 12-byte nonce is 4 zero bytes followed by the 64-bit record counter,
  big-endian for AES-GCM and little-endian for ChaCha20-Poly1305.
- Each direction rekeys after every 16,384 records, `FINISH` included: the new
  key is the first 32 bytes of 32 zero bytes encrypted at nonce 2^64−1. The
  counter continues. At full records that is about 1 GiB per key.
- A counter never repeats under a key. Nonce 2^64−1 is reserved for rekeying.

A full record seals straight from the writer's caller and opens straight
into a reader's buffer that can hold its payload; other records seal and open
in place in pooled record buffers, 256 KiB for writing and 128 KiB for
reading. A reader asks each receive for a record's body and the next record's
header together, so a stream of full records costs one receive per record and
a record never has to be moved within the buffer. Bytes read past a message's
`END` stay buffered for the next message. A reader takes its buffer only once
a message's first byte arrives, so an idle connection holds none. The record path makes no
allocations per record or per message. Each direction is
guarded by its own lock for each read, write, flush, or end. Commands do not
interleave: a connection pool lends a connection to one borrower at a time.

### Writing

A writer accumulates bytes regardless of its caller's write boundaries:

- A record is sealed when it reaches 65,517 payload bytes, so a full record,
  header included, is exactly 64 KiB. The last record before a write may be
  shorter, to fit the 256 KiB write buffer. When a write holds a whole
  record's payload and none is pending, that record is sealed from the
  caller's slice rather than copied first.
- Four sealed records share one socket write. A gentle `SEND` response writes
  each record as it is sealed, so its rate limiter keeps its pacing.
- `Flush` seals pending bytes as a short `DATA` record and writes everything
  queued; with nothing pending it seals nothing.
- `End` seals the final `END` record and writes everything.

Responses flush at progress points, so a peer sees progress without waiting for
a full record: after each completed non-terminal framed-body frame (manifests,
`SYNC`, `STATUS`), at most every 250 ms between `CXSUM` frames, and after each
gentle `SEND` frame. Ordinary writes never force a record. An encrypted `SEND`
always goes through the record writer; only plaintext responses use sendfile.

## Command Completion

### Requests

The server acts on a request only after the request has ended exactly: every
byte its grammar expects is read, and the next read is its authenticated `END`.
Framed-body parsers read from the same buffer as the command line, so trailing
bytes cannot hide in a second read-ahead buffer.

| Request                                        | Completion point                                                                                |
|------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `TXFER`, `STATUS`, and other bodyless commands | After the command line, before dispatch                                                         |
| `SEND`, `ACK`, `CXSUM`                         | After the framed item body verifies, before any item is applied                                 |
| `SYNC`                                         | After the manifest terminator, the framed body's end, and `END`, before the transfer is created |
| `PROBE`                                        | After the declared payload and `END`, before the limiter or keep-alive grant changes            |

A request that does not end at its `END` closes the connection without a
response. A request that ends correctly but fails gets an encrypted `ERR`
response. If a handler fails before reading the body, the server reads the
rest of the request, at most the largest request any command may send, and
then answers. A command line that does not parse is answered with `ERR` only
when the message ends right after it; otherwise, or when the line is too long
or unterminated, the server closes the connection, since the rest of an
unparsed request cannot be trusted.

A `PROBE` that requests keep-alive advertises the grant it is about to make,
and the grant takes effect only after the command succeeds.

### Responses

A client accepts a response, and returns its connection to a pool, only after
reading the response's terminal status line and then `END` through the
outermost buffer it parsed with. A `SEND` or checksum stream returns EOF only
after verifying `END`, and a `SEND` response that ends without its terminal
`OK` is an error. `Close` reports any such failure, including one a caller
that stopped reading early never saw. A checksum stream also verifies `END`
when it closes, even if its caller consumed the status line without another
read; when its caller set no response deadline, it waits at most five
seconds for that `END`. Other commands wait for `END` under their caller's
context, which closes the connection when it is cancelled.

## Admission and Deadlines

### Server

`ServerOptions.MaxUnauthenticatedConns` (default 1024) bounds connections that
have not finished authenticating. A server that requires authentication takes
an admission permit at accept; a server without flags takes one when a
connection's first line is an `AUTH` form, so on such a server plaintext
commands never take one.
The permit is held until `FINISH` verifies or the connection closes. A
connection that cannot get a permit receives `ERR BUSY`, written with a 10 ms
deadline; the server then half-closes, reads at most 354 bytes of the client's
opening (an `AUTH` line and message 1) for at most 200 ms, and closes. The accept loop never waits for a permit or a refusal.

The cap bounds sockets, memory, and descriptors until `FINISH`; after it, the
idle timeout bounds how long a silent session lives. It deliberately does not
track CPU count: a small cap would let a few idle sockets exclude every client. It
does not bound handshake CPU or connection floods. Every opening costs the
server one X25519 operation and one HMAC per allowlist entry of its mode before
the tag can match, and an opening whose tag matches costs several more X25519
operations before `FINISH`.

One absolute deadline, five seconds from accept, covers the discovery reply,
handshake, and `FINISH`. On a server that requires authentication it also
covers reading the first line, which is limited to 256 bytes. A server without
flags reads the first line as it reads any plaintext command, under the idle
progress deadline and the command-line limit, and applies the five-second
deadline once that line is an `AUTH` form.

After `FINISH`, each socket read has a progress deadline equal to the server's
idle timeout (`--idle-timeout`, default 60 s; 60 s when keep-alive is
disabled). It covers the first command, the time between commands, and the
time between bytes of one request, even within a record, so a large request
that keeps arriving is never cut off, while a silent session is closed.

### Client

- A client runs at most 16 handshakes at once, shared by its connection pools,
  discovery, and token selection, so filling a pool cannot take a server's
  whole admission capacity.
- `ERR BUSY` is retried with the same credential on a fresh connection, up to
  four times. The backoff starts at 50 ms and doubles to 400 ms, and each wait
  is jittered down by up to half: 25–50 ms first, 200–400 ms at most. The
  client gives up its handshake slot while it waits.
- Each authentication has one deadline: five seconds, or the caller's context
  deadline if earlier. Retries and token attempts share it.
- Cancelling a command's context closes its connection, which ends any read
  waiting on a status line or `END`; the connection never returns to a pool.

## Client Credential Selection

- A client with a token, or with an age identity, encrypts; its default cipher
  setting is `auto`, and `none` is an error. A client with neither uses
  plaintext unless it asks for encryption.
- A client with tokens uses token mode. A client with an age identity and no
  token uses age mode. A client asking for encryption with neither uses age
  mode with its local key.
- Without a configured identity, a client generates one in-memory key pair and
  uses it whenever it encrypts. It never writes that key to disk.
- A recipient without its identity, or an identity that does not match a
  configured recipient, is an error before any connection opens.

### Token candidates

A client may hold several tokens, tried in order. Each attempt uses a fresh
connection and handshake. The first token whose message 2 authenticates is
used for the connection, and later connections start with it. Selection moves
to the next token only when a token is refused or the server ends the
handshake before message 2; `ERR BUSY` retries the same token, and the
not-configured diagnostic ends selection. A message 2 that fails
authentication, a server recipient mismatch, an expired deadline, a
cancellation, or a failed dial ends the attempt without trying another token.

## Server Keys

A server proves possession of its age identity in every handshake.
`ServerOptions.ServerIdentity` supplies it; a library server without one
generates a key at startup.

The CLI keeps its identity in the keys directory (`--keys`, default
`/var/lib/tx/keys`):

- If the directory holds a key, the server loads it. An invalid key fails
  startup.
- If the directory has no key, the server creates one there, crash-safely: it
  writes a private (`0600`) temporary file, syncs it, and publishes it with
  `link(2)`, which never overwrites an existing key. A crash leaves at most a
  stray temporary file, which is ignored. If another process publishes a key
  first, the server loads that key.
- If `--keys` is omitted, or names the default directory, and that directory
  is missing or not writable, the server uses an ephemeral key and logs that
  it changes on restart. Any other directory that is missing or not writable
  fails startup.

A client caches the discovered key for its lifetime, so after the server's key
changes, a running client fails new handshakes until it restarts. A client that
requires the server's recipient must be given the new one.

## Security Properties

- Neither tokens nor private keys cross the wire. A token is proven through
  the PSK.
- Session keys come from ephemeral key exchange, so recorded traffic stays
  confidential even if a static key or token later leaks.
- Every command runs only after `FINISH` proves that the client derived this
  session's keys. A replayed opening never authorizes work.
- Someone holding the server's private key can forge an age-mode message 1 for
  any client, but cannot produce that client's `FINISH` without the client's
  private key. Token mode additionally requires the token.
- The server proves the key it advertised, not its identity. Unless the client
  requires the server's recipient, an active attacker can substitute its own
  key during discovery. Against a server that accepts any key, such a client
  is then protected only from passive observers. In token mode, any holder of
  the same token can impersonate the server to other clients that do not
  require its recipient. Use one token per client.
- The credential tag is keyed by `es`, so a passive observer cannot test token
  guesses against it or link a client's connections. An active attacker who
  substitutes its own key for a client that does not require the server's
  recipient learns `es` and can test guesses of that client's token offline,
  so short tokens call for requiring the server's recipient. Anyone who knows a recipient can still build its tag and probe
  whether it is allowlisted, though without its private key they cannot
  complete the handshake.
- Authentication does not scope access: any accepted credential can use every
  transfer on the server.
- Logs, errors, and events never contain tokens, PSKs, or private keys.

## Cost

- A client performs one discovery round trip per client, and each new
  connection one handshake round trip. Pooled connections reuse their session.
  With keep-alive disabled, every command pays the handshake.
- Each side performs six X25519 operations per handshake: Noise's five, plus
  `es` once more for the credential tag.
- A full record adds 19 bytes per 64 KiB: the 2-byte length, the kind byte, and
  the 16-byte tag.

## Limits

| Item                                     | Limit                                      |
|------------------------------------------|--------------------------------------------|
| `AUTH` line, discovery reply, diagnostic | 256 bytes                                  |
| Handshake message 1 / message 2          | 96 / 48 bytes                              |
| Record ciphertext                        | 16–65,535 bytes                            |
| Emitted record payload                   | 65,517 bytes                               |
| Records per key                          | 16,384                                     |
| Handshake deadline                       | 5 s from accept (server) or start (client) |
| Unauthenticated connections              | 1024 by default                            |
| Client handshakes in flight              | 16                                         |
| `ERR BUSY` retries                       | 4                                          |

## Versioning

- `noise-v1` is the current version, and its strings are fixed: the
  discovery line, the preface, the diagnostics, and the record kinds. Any
  change to the preface, handshake, or record format, including a new cipher
  or record kind, requires a new version string in both the discovery line and
  the preface. A server that speaks several versions answers each version's
  discovery line and preface.
- There is no negotiation and no fallback: a client with credentials never
  sends plaintext or legacy `AUTH`, and a server answers legacy `AUTH` forms
  with `ERR UNSUPPORTED_AUTH`.
- Clients from before `noise-v1` that encrypt fail at discovery and print
  "client predates noise-v1; upgrade tx"; the server logs their address.
  Clients that do not encrypt are unaffected.

## References

- [Noise Protocol Framework](https://noiseprotocol.org/noise.html), including
  [pre-shared symmetric keys](https://noiseprotocol.org/noise.html#pre-shared-symmetric-keys)
- [flynn/noise v1.1.0](https://github.com/flynn/noise/tree/v1.1.0)
- [age](https://age-encryption.org/v1)
