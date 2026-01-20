"""
ready.py

Servidor HTTPS minimo con /machine/health para exponer "las dos machines estan arriba".

Comportamiento:
- GET /machine/health -> 200 si machine-a y machine-b responden OK a /machine/health
- En otro caso -> 503

Expuesto en puerto 5000 (HTTPS).
"""

import os
from http.server import BaseHTTPRequestHandler, HTTPServer
import ssl
import urllib.request
import urllib.error


MACHINE_A_URL = "https://machine-a:5001/machine/health"
MACHINE_B_URL = "https://machine-b:5001/machine/health"

# Certificados (mismos que usan las machines)
CERT_FILE = os.getenv("SERVICE_CERT_FILE", "/certs/machine/machine-cert.pem")
KEY_FILE = os.getenv("SERVICE_KEY_FILE", "/certs/machine/machine-key.pem")


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
    """Handler minimo para /machine/health."""

    def do_GET(self):
        """Implementa GET /machine/health."""
        if self.path != "/machine/health":
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
    """Arranque del servidor HTTPS."""
    server = HTTPServer(("0.0.0.0", 5000), Handler)
    
    # Envolver con SSL
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    
    print(f"Ready server listening on HTTPS port 5000...")
    server.serve_forever()
