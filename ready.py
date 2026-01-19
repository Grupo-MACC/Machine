"""
ready.py

Servidor HTTP minimo con /health para exponer "las dos machines estan arriba".

Comportamiento:
- GET /health -> 200 si machine-a y machine-b responden OK a /machine/health
- En otro caso -> 503
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import ssl
import urllib.request
import urllib.error


MACHINE_A_URL = "https://machine-a:5001/machine/health"
MACHINE_B_URL = "https://machine-b:5001/machine/health"


def _check(url: str) -> bool:
    """
    Comprueba si un endpoint HTTPS responde.
    - Desactiva verificacion TLS (cert self-signed en lab / aws demo)
    """
    ctx = ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(url, context=ctx, timeout=2) as r:
            return 200 <= r.status < 300
    except (urllib.error.URLError, TimeoutError, Exception):
        return False


class Handler(BaseHTTPRequestHandler):
    """Handler minimo para /health."""

    def do_GET(self):
        """Implementa GET /health."""
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found")
            return

        a_ok = _check(MACHINE_A_URL)
        b_ok = _check(MACHINE_B_URL)

        if a_ok and b_ok:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK: machine-a and machine-b are healthy")
        else:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(
                f"NOT_READY: a={a_ok} b={b_ok}".encode("utf-8")
            )


if __name__ == "__main__":
    """Arranque del servidor."""
    server = HTTPServer(("0.0.0.0", 8080), Handler)
    server.serve_forever()
