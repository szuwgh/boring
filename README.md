# boring

HTTP forward proxy supports HTTP requests and HTTPS CONNECT tunnels. The machine
running boring must be able to reach the requested destinations.

Copy `examples/http-proxy.toml` to `boring.toml` in the working directory.
Set `allow_clients` to the client machines' CIDRs (IPv4 `/32`, IPv6 `/128`).
`listen` is the proxy machine's listening interface, not the client's address.
`allowed_ports` defaults to `[80, 443]`; an explicit empty list is invalid.
Set `allow_any_port = true` to ignore `allowed_ports` and allow all valid TCP ports.
Client restrictions still apply. No fixed `host` or `target` is needed.
The existing `connect_forward` mode remains available for fixed destinations.

Build and run:

```bash
go build -o boring .
./boring
# Cross-compile for Windows:
GOOS=windows GOARCH=amd64 go build -o boring.exe .
```

Allow inbound TCP 8081 from the configured client through the host firewall.
For proxy host `192.168.1.50`, set these on the Linux client:

```bash
export http_proxy=http://192.168.1.50:8081
export https_proxy=http://192.168.1.50:8081
export no_proxy=localhost,127.0.0.1,::1
curl -v http://example.com/
curl -v https://example.com/
```

For explicit verification, use `curl --proxy http://192.168.1.50:8081 URL`.
HTTPS uses CONNECT without decrypting TLS. DNS resolves on the proxy machine.
Only applications supporting proxy settings use these environment variables;
this does not provide system-wide transparent routing, UDP, or SOCKS.
