# gunicorn.conf.py
workers = 1
worker_class = "gthread"   # estable para SSE
threads = 8
timeout = 0                # sin timeout del worker (clave para SSE)
keepalive = 5
graceful_timeout = 30
log_level = "info"

def post_fork(server, worker):
    server.log.info(
        ">>> Gunicorn OK: worker_class=%s threads=%s timeout=%s keepalive=%s",
        worker.cfg.worker_class, worker.cfg.threads, worker.cfg.timeout, worker.cfg.keepalive
    )
