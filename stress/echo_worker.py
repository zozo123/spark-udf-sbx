import os
import socket
import sys
import threading

args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
path = args["--connection"]
try:
    os.unlink(path)
except FileNotFoundError:
    pass
server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(path)
server.listen(64)
print(f"echo_worker {args.get('--id')} listening on {path}", file=sys.stderr, flush=True)


def serve(conn):
    while True:
        data = conn.recv(65536)
        if not data:
            break
        conn.sendall(data)
    conn.close()


while True:
    conn, _ = server.accept()
    threading.Thread(target=serve, args=(conn,), daemon=True).start()
