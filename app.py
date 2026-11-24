import os
from mcp.server.transport_security import TransportSecuritySettings
from mailroom import build_server, init_db

init_db()
SECRET = os.environ["PLANGRID_SECRET"]
mcp = build_server(stateless_http=True, json_response=True, streamable_http_path=f"/{SECRET}/mcp",
                   transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))
_inner = mcp.streamable_http_app()

async def app(scope, receive, send):
    if scope["type"] == "http" and scope["path"] == "/health":
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": b"ok"})
        return
    await _inner(scope, receive, send)
