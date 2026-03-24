#!/usr/bin/env python3
import json
import re
import subprocess


BASE = "https://bratstvokoltsa.com"
ALLOWED_INIT_DATA = "user=%7B%22id%22%3A8794843839%2C%22first_name%22%3A%22Admin%22%7D"


def curl_code(url: str, method: str = "GET", payload=None):
    cmd = ["curl", "-sS", "-o", "/tmp/fullscan_body", "-w", "%{http_code}", "-X", method, url]
    if payload is not None:
        cmd += ["-H", "Content-Type: application/json", "--data-raw", json.dumps(payload, separators=(",", ":"))]
    return subprocess.check_output(cmd, text=True).strip()


def main():
    html = subprocess.check_output(["curl", "-fsS", BASE + "/"], text=True, encoding="utf-8", errors="replace")
    paths = set(re.findall(r"/functions/v1/[a-zA-Z0-9_\\-]+", html))
    paths.update(re.findall(r"/ws\\b", html))

    static_pat = r'"(/[^"\\s]+\.(?:png|jpg|jpeg|webp|gif|svg|ico|css|js|json|woff2?|ttf))"'
    for m in re.findall(static_pat, html):
        paths.add(m)
    for m in re.findall(r'(?:src|href)="(/[^"\\s]+)"', html):
        low = m.lower()
        if low.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg", ".ico", ".css", ".js", ".json", ".woff", ".woff2", ".ttf")):
            paths.add(m)

    results = []
    for p in sorted(paths):
        if p.startswith("/functions/v1/"):
            code = curl_code(BASE + p, method="POST", payload={"initData": ALLOWED_INIT_DATA})
        elif p == "/ws":
            code = "WS"
        else:
            code = curl_code(BASE + p, method="GET")
        results.append((p, code))

    bad = [x for x in results if x[1] not in ("200", "204", "WS")]
    print("TOTAL_PATHS={0}".format(len(results)))
    for p, code in results:
        print("{0} {1}".format(code, p))
    print("BAD_COUNT={0}".format(len(bad)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
