#!/usr/bin/env python3
import html.parser
import json
import os
import platform
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


DEFAULT_TIMEOUT = 10
DEFAULT_START_TIMEOUT = 3
DEFAULT_MIRROR_MAX_TIME = 10
DEFAULT_SAMPLE_MB = 10
DEFAULT_WORKERS = 0
DEFAULT_KIND = "docker"
DEFAULT_DOCKER_TARGET = "homebrew/brew:latest"
DEFAULT_GITHUB_TARGET = "https://github.com/cli/cli/releases/download/v2.74.2/gh_2.74.2_linux_amd64.tar.gz"
DEFAULT_GENERIC_TARGET = "/ubuntu/dists/noble/main/binary-amd64/Packages.gz"
DEFAULT_PIP_TARGET = "pip"
DEFAULT_NPM_TARGET = "lodash"
DEFAULT_MAVEN_TARGET = "org.apache.commons:commons-lang3:3.14.0"
DEFAULT_GO_TARGET = "github.com/gin-gonic/gin@latest"
DEFAULT_CARGO_TARGET = "serde@latest"
DEFAULT_NUGET_TARGET = "Newtonsoft.Json@13.0.3"
DEFAULT_CONDA_TARGET = "numpy"
DEFAULT_HOMEBREW_TARGET = "/api/formula.jws.json"
DEFAULT_GIT_TARGET = "git/git"
DEFAULT_APT_TARGET = "/ubuntu/dists/noble/main/binary-amd64/Packages.gz"
DEFAULT_YUM_TARGET = "/centos-stream/9-stream/BaseOS/x86_64/os/repodata/repomd.xml"
DEFAULT_APK_TARGET = "/alpine/latest-stable/main/x86_64/APKINDEX.tar.gz"
DEFAULT_FLATPAK_TARGET = "/repo/summary"
USER_AGENT = "mirror-speed-test/2.0"
RELEASE_REPO_OWNER = "fa1seut0pia"
RELEASE_REPO_NAME = "mirror-speed-test"
RELEASE_API_URL = f"https://api.github.com/repos/{RELEASE_REPO_OWNER}/{RELEASE_REPO_NAME}/releases/latest"
RELEASE_PAGE_URL = f"https://github.com/{RELEASE_REPO_OWNER}/{RELEASE_REPO_NAME}/releases"


def _resource_root_dir():
    if getattr(sys, "frozen", False):
        return getattr(sys, "_MEIPASS", os.path.dirname(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def load_app_version():
    env_ver = str(os.environ.get("MST_VERSION", "")).strip()
    if env_ver:
        return env_ver
    candidates = [
        os.path.join(_resource_root_dir(), "VERSION"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "VERSION"),
    ]
    for path in candidates:
        try:
            with open(path, "r", encoding="utf-8") as f:
                value = f.read().strip()
                if value:
                    return value
        except Exception:
            continue
    return "dev"


APP_VERSION = load_app_version()


def build_ssl_context():
    try:
        import certifi

        cafile = certifi.where()
        if cafile and os.path.exists(cafile):
            return ssl.create_default_context(cafile=cafile)
    except Exception:
        pass
    return ssl.create_default_context()


SSL_CONTEXT = build_ssl_context()
HEAD_FALLBACK_CODES = {400, 403, 405, 501}
MANIFEST_ACCEPT = ", ".join(
    [
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    ]
)


class MirrorTestError(Exception):
    pass


def log_line(level, message):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{ts}] [{level}] {message}", flush=True)


def log_info(message):
    log_line("INFO", message)


def log_warn(message):
    log_line("WARN", message)


def log_error(message):
    log_line("ERROR", message)


def make_request(url, headers=None, method="GET"):
    request = urllib.request.Request(url, headers=headers or {}, method=method)
    request.add_header("User-Agent", USER_AGENT)
    return request


def open_url(url, headers=None, method="GET", timeout=DEFAULT_START_TIMEOUT):
    request = make_request(url, headers=headers, method=method)
    return urllib.request.urlopen(request, timeout=timeout, context=SSL_CONTEXT)


def is_http_url(value):
    parsed = urllib.parse.urlparse(str(value).strip())
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)


def filename_from_url(url):
    parsed = urllib.parse.urlparse(url)
    name = parsed.path.rsplit("/", 1)[-1]
    return urllib.parse.unquote(name) or url


def target_path_for_template(target):
    target = str(target).strip()
    if not is_http_url(target):
        return target.lstrip("/")
    parsed = urllib.parse.urlparse(target)
    path = f"{parsed.netloc}{parsed.path}"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    return path.lstrip("/")


def render_mirror_target(mirror, target, allow_full_url_prefix=False):
    mirror = str(mirror).strip().rstrip("/")
    target = str(target).strip()
    target_url = target if is_http_url(target) else target.lstrip("/")
    target_path = target_path_for_template(target)

    if allow_full_url_prefix and is_http_url(target) and is_http_url(mirror):
        target_parsed = urllib.parse.urlparse(target)
        mirror_parsed = urllib.parse.urlparse(mirror)
        if (
            target_parsed.netloc.lower() == mirror_parsed.netloc.lower()
            and (mirror_parsed.path or "").strip("/") == ""
        ):
            return target

    if "{url}" in mirror or "{path}" in mirror:
        return mirror.replace("{url}", target_url).replace("{path}", target_path)
    if is_http_url(target):
        if not allow_full_url_prefix:
            raise MirrorTestError("完整 URL 目标需要在镜像地址中使用 {url} 或 {path} 模板")
        return f"{mirror}/{target_url}"
    return urllib.parse.urljoin(f"{mirror}/", target.lstrip("/"))


def fetch_text(url, timeout=DEFAULT_START_TIMEOUT):
    try:
        with open_url(url, timeout=timeout) as response:
            content = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            return content.decode(charset, errors="replace")
    except urllib.error.HTTPError as error:
        raise MirrorTestError(f"HTTP {error.code} from {url}") from error
    except urllib.error.URLError as error:
        raise MirrorTestError(f"request failed: {error}") from error


def fetch_text_with_latency(url, timeout=DEFAULT_START_TIMEOUT):
    started = time.perf_counter()
    content = fetch_text(url, timeout=timeout)
    latency_ms = round((time.perf_counter() - started) * 1000, 1)
    return content, latency_ms


def fetch_json(url, timeout=DEFAULT_START_TIMEOUT):
    try:
        with open_url(url, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        raise MirrorTestError(f"HTTP {error.code} from {url}") from error
    except urllib.error.URLError as error:
        raise MirrorTestError(f"request failed: {error}") from error
    except json.JSONDecodeError as error:
        raise MirrorTestError(f"invalid JSON from {url}: {error}") from error


def natural_version_key(text):
    parts = re.split(r"(\d+)", str(text).strip().lower())
    key = []
    for part in parts:
        if not part:
            continue
        if part.isdigit():
            key.append((0, int(part)))
        else:
            key.append((1, part))
    return tuple(key)


def parse_version_tuple(value):
    text = str(value or "").strip()
    match = re.match(r"^v?(\d+)(?:\.(\d+))?(?:\.(\d+))?", text)
    if not match:
        return None
    major = int(match.group(1) or 0)
    minor = int(match.group(2) or 0)
    patch = int(match.group(3) or 0)
    return major, minor, patch


def is_version_newer(latest, current):
    latest_tuple = parse_version_tuple(latest)
    current_tuple = parse_version_tuple(current)
    if latest_tuple is not None and current_tuple is not None:
        return latest_tuple > current_tuple
    if latest_tuple is not None and current_tuple is None:
        return False
    latest_text = str(latest or "").strip().lower()
    current_text = str(current or "").strip().lower()
    if not latest_text or not current_text:
        return False
    return natural_version_key(latest_text) > natural_version_key(current_text)


def detect_release_asset_name():
    machine = platform.machine().lower()
    if machine not in ("x86_64", "amd64"):
        return None, f"unsupported architecture: {machine}"
    if sys.platform.startswith("linux"):
        return "mirror-speed-test-linux-x64", ""
    if sys.platform == "win32":
        return "mirror-speed-test-windows-x64.exe", ""
    if sys.platform == "darwin":
        return "mirror-speed-test-macos-x64", ""
    return None, f"unsupported platform: {sys.platform}"


def fetch_latest_release(timeout=10):
    headers = {"Accept": "application/vnd.github+json"}
    try:
        with open_url(RELEASE_API_URL, headers=headers, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        raise MirrorTestError(f"HTTP {error.code} from release API") from error
    except urllib.error.URLError as error:
        raise MirrorTestError(f"release API request failed: {error}") from error
    except json.JSONDecodeError as error:
        raise MirrorTestError(f"invalid release API JSON: {error}") from error


def build_update_status():
    current_version = APP_VERSION
    is_binary = bool(getattr(sys, "frozen", False))
    expected_asset, asset_reason = detect_release_asset_name()

    payload = {
        "current_version": current_version,
        "release_page": RELEASE_PAGE_URL,
        "platform_message": asset_reason or "",
        "latest_version": "",
        "release_url": RELEASE_PAGE_URL,
        "has_update": False,
        "can_download": False,
        "can_auto_update": False,
        "download_message": "",
    }

    release = fetch_latest_release()
    latest_version = str(release.get("tag_name") or "").strip()
    release_url = str(release.get("html_url") or RELEASE_PAGE_URL)
    payload["latest_version"] = latest_version
    payload["release_url"] = release_url

    asset_url = ""
    for asset in (release.get("assets") or []):
        if str(asset.get("name") or "").strip() == expected_asset:
            asset_url = str(asset.get("browser_download_url") or "").strip()
            break
    payload["asset_url"] = asset_url

    if latest_version and current_version:
        payload["has_update"] = is_version_newer(latest_version, current_version)
    payload["can_download"] = bool(expected_asset and asset_url)
    payload["can_auto_update"] = bool(
        payload["has_update"] and payload["can_download"] and is_binary and sys.platform != "win32"
    )
    if not expected_asset:
        payload["download_message"] = asset_reason or "当前平台暂不支持自动下载"
    elif not asset_url:
        payload["download_message"] = f"最新版本未找到构建产物：{expected_asset}"
    return payload


def _download_to_path(url, path, timeout=30):
    tmp_path = path + ".part"
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    with open_url(url, timeout=timeout) as response, open(tmp_path, "wb") as f:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
    os.replace(tmp_path, path)
    return path


def prepare_update_download(preferred_mirror="", preferred_speed_mbps=None):
    update = build_update_status()
    if not update.get("has_update"):
        return {"ok": True, "message": "当前已是最新版本", "status": update, "download_url": ""}
    if not update.get("asset_url"):
        raise MirrorTestError("当前平台未匹配到可下载的构建产物")
    download_url = update["asset_url"]
    best_mirror = str(preferred_mirror or "").strip().rstrip("/")
    best_speed = preferred_speed_mbps

    github_cfg = get_runtime_config_snapshot().get("github", {})
    mirrors = dedupe_mirrors(github_cfg.get("mirrors", []) or [])
    if best_mirror and best_mirror in mirrors:
        download_url = render_mirror_target(
            best_mirror,
            update["asset_url"],
            allow_full_url_prefix=True,
        )
    elif mirrors:
        try:
            test_payload = run_test_batch(
                kind="github",
                mirrors=mirrors,
                target=update["asset_url"],
                sample_mb=1,
            )
            ok_list = [item for item in (test_payload.get("results") or []) if item.get("ok")]
            if ok_list:
                best = ok_list[0]
                best_mirror = str(best.get("mirror") or "").strip()
                best_speed = (best.get("download") or {}).get("speed_mbps")
                if best_mirror:
                    download_url = render_mirror_target(
                        best_mirror,
                        update["asset_url"],
                        allow_full_url_prefix=True,
                    )
        except Exception as error:
            log_warn(f"update mirror benchmark failed: {error}")

    payload = {
        "ok": True,
        "message": f"已生成 {update.get('latest_version') or '最新版本'} 的下载链接",
        "latest_version": update.get("latest_version") or "",
        "download_url": download_url,
        "best_mirror": best_mirror,
        "best_speed_mbps": best_speed,
        "open_in_browser": True,
        "restart_required": False,
        "updated": False,
        "status": update,
    }

    if update.get("can_auto_update"):
        exe_path = os.path.abspath(sys.executable)
        download_path = exe_path + ".new"
        _download_to_path(download_url, download_path, timeout=max(DEFAULT_TIMEOUT, 30))
        try:
            current_mode = os.stat(exe_path).st_mode
            os.chmod(download_path, current_mode | 0o111)
        except Exception:
            pass

        backup_path = exe_path + ".bak"
        try:
            if os.path.exists(backup_path):
                os.remove(backup_path)
            os.replace(exe_path, backup_path)
        except Exception:
            backup_path = ""
        os.replace(download_path, exe_path)

        payload["message"] = f"已更新到 {update.get('latest_version') or '最新版本'}，请重启程序生效"
        payload["backup_path"] = backup_path
        payload["restart_required"] = True
        payload["open_in_browser"] = False
        payload["updated"] = True
    return payload


def ping_url(url, timeout=DEFAULT_START_TIMEOUT):
    start = time.perf_counter()
    try:
        with open_url(url, method="HEAD", timeout=timeout) as response:
            elapsed = time.perf_counter() - start
            return {
                "ok": True,
                "status": response.status,
                "latency_ms": round(elapsed * 1000, 1),
            }
    except urllib.error.HTTPError as error:
        if error.code not in HEAD_FALLBACK_CODES:
            raise MirrorTestError(f"ping failed: HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise MirrorTestError(f"ping failed: {error}") from error

    start = time.perf_counter()
    try:
        with open_url(url, headers={"Range": "bytes=0-0"}, timeout=timeout) as response:
            response.read(1)
            elapsed = time.perf_counter() - start
            return {
                "ok": True,
                "status": response.status,
                "latency_ms": round(elapsed * 1000, 1),
            }
    except urllib.error.HTTPError as error:
        if error.code == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
            elapsed = time.perf_counter() - start
            return {
                "ok": True,
                "status": error.code,
                "latency_ms": round(elapsed * 1000, 1),
            }
        raise MirrorTestError(f"ping failed: HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise MirrorTestError(f"ping failed: {error}") from error


def _set_response_socket_timeout(response, timeout):
    if timeout is None:
        return
    try:
        raw = getattr(getattr(response, "fp", None), "raw", None)
        sock = getattr(raw, "_sock", None) or getattr(raw, "sock", None)
        if sock is not None:
            sock.settimeout(timeout)
    except Exception:
        return


def download_sample_url(
    url,
    sample_bytes,
    timeout=DEFAULT_TIMEOUT,
    start_timeout=DEFAULT_START_TIMEOUT,
    max_duration=DEFAULT_MIRROR_MAX_TIME,
):
    headers = {"Range": f"bytes=0-{sample_bytes - 1}"}
    start = time.perf_counter()
    deadline = start + max_duration
    first_byte_at = None
    total_bytes = 0
    response_headers = {}
    status = None
    final_url = url
    try:
        with open_url(url, headers=headers, timeout=start_timeout) as response:
            status = response.status
            response_headers = dict(response.headers)
            final_url = response.geturl()
            while total_bytes < sample_bytes:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    break
                if first_byte_at is not None:
                    _set_response_socket_timeout(response, min(timeout, max(0.5, remaining)))
                chunk_size = min(64 * 1024, sample_bytes - total_bytes)
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                if first_byte_at is None:
                    first_byte_at = time.perf_counter()
                    _set_response_socket_timeout(
                        response,
                        min(timeout, max(0.5, deadline - first_byte_at)),
                    )
                total_bytes += len(chunk)
    except urllib.error.HTTPError as error:
        raise MirrorTestError(f"HTTP {error.code} from {url}") from error
    except urllib.error.URLError as error:
        raise MirrorTestError(f"request failed: {error}") from error
    except (OSError, TimeoutError) as error:
        if first_byte_at is None:
            raise MirrorTestError(f"download start timeout (>{start_timeout}s): {error}") from error
        raise MirrorTestError(f"download interrupted: {error}") from error

    finished_at = time.perf_counter()
    if first_byte_at is None and total_bytes <= 0:
        raise MirrorTestError(f"download start timeout (>{start_timeout}s): no data received")
    total_time = finished_at - start
    first_byte_time = (first_byte_at - start) if first_byte_at is not None else total_time
    speed_bps = total_bytes / total_time if total_time > 0 else 0.0
    return {
        "status": status,
        "url": final_url,
        "bytes_downloaded": total_bytes,
        "time_total_s": round(total_time, 3),
        "time_to_first_byte_s": round(first_byte_time, 3),
        "speed_bps": speed_bps,
        "speed_mbps": round(speed_bps / 1024 / 1024, 2),
        "content_length": response_headers.get("Content-Length"),
        "content_range": response_headers.get("Content-Range"),
        "accept_ranges": response_headers.get("Accept-Ranges"),
        "time_capped": total_bytes < sample_bytes,
    }


def make_base_result(kind, mirror, target, sample_bytes):
    started_at = time.time()
    return {
        "kind": kind,
        "mirror": mirror,
        "target": target,
        "sample_bytes": sample_bytes,
        "tested_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(started_at)),
    }


class RegistryClient:
    def __init__(
        self,
        mirror,
        timeout=DEFAULT_TIMEOUT,
        start_timeout=DEFAULT_START_TIMEOUT,
        max_duration=DEFAULT_MIRROR_MAX_TIME,
    ):
        self.mirror = mirror.rstrip("/")
        self.timeout = timeout
        self.start_timeout = start_timeout
        self.max_duration = max_duration
        self.token_cache = {}

    def _open(self, url, headers=None, method="GET", timeout=None):
        if timeout is None:
            timeout = self.start_timeout
        return open_url(url, headers=headers, method=method, timeout=timeout)

    def ping(self):
        start = time.perf_counter()
        try:
            with self._open(f"{self.mirror}/v2/") as response:
                response.read(1)
                elapsed = time.perf_counter() - start
                return {
                    "ok": True,
                    "status": response.status,
                    "latency_ms": round(elapsed * 1000, 1),
                }
        except urllib.error.HTTPError as error:
            elapsed = time.perf_counter() - start
            if error.code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.OK):
                return {
                    "ok": True,
                    "status": error.code,
                    "latency_ms": round(elapsed * 1000, 1),
                }
            raise MirrorTestError(f"/v2/ returned HTTP {error.code}") from error
        except Exception as error:
            raise MirrorTestError(f"ping failed: {error}") from error

    def _parse_www_authenticate(self, header_value):
        if not header_value:
            return None
        match = re.match(r"Bearer\s+(.*)", header_value, re.IGNORECASE)
        if not match:
            return None
        params = {}
        for key, value in re.findall(r'(\w+)="([^"]+)"', match.group(1)):
            params[key] = value
        realm = params.get("realm")
        if not realm:
            return None
        return {
            "realm": realm,
            "service": params.get("service"),
            "scope": params.get("scope"),
        }

    def _get_token(self, auth_info, scope_override=None):
        scope = scope_override or auth_info.get("scope") or ""
        cache_key = (auth_info["realm"], auth_info.get("service"), scope)
        if cache_key in self.token_cache:
            return self.token_cache[cache_key]

        query = {}
        if auth_info.get("service"):
            query["service"] = auth_info["service"]
        if scope:
            query["scope"] = scope
        token_url = auth_info["realm"]
        if query:
            token_url = f"{token_url}?{urllib.parse.urlencode(query)}"

        with self._open(token_url) as response:
            payload = json.loads(response.read().decode("utf-8"))
        token = payload.get("token") or payload.get("access_token")
        if not token:
            raise MirrorTestError("token endpoint returned no token")
        self.token_cache[cache_key] = token
        return token

    def _request_json(self, url, headers=None, expected_status=200, scope=None):
        headers = headers or {}
        try:
            with self._open(url, headers=headers) as response:
                if response.status != expected_status:
                    raise MirrorTestError(f"unexpected HTTP {response.status} from {url}")
                return json.loads(response.read().decode("utf-8")), dict(response.headers)
        except urllib.error.HTTPError as error:
            if error.code != HTTPStatus.UNAUTHORIZED:
                raise MirrorTestError(f"HTTP {error.code} from {url}") from error
            auth_info = self._parse_www_authenticate(error.headers.get("WWW-Authenticate"))
            if not auth_info:
                raise MirrorTestError("registry requires auth but sent no Bearer challenge") from error
            token = self._get_token(auth_info, scope_override=scope)
            retry_headers = dict(headers)
            retry_headers["Authorization"] = f"Bearer {token}"
            with self._open(url, headers=retry_headers) as response:
                if response.status != expected_status:
                    raise MirrorTestError(f"unexpected HTTP {response.status} from {url}")
                return json.loads(response.read().decode("utf-8")), dict(response.headers)
        except urllib.error.URLError as error:
            raise MirrorTestError(f"request failed: {error}") from error

    def _request_response(self, url, headers=None, scope=None, timeout=None):
        headers = headers or {}
        try:
            return self._open(url, headers=headers, timeout=timeout)
        except urllib.error.HTTPError as error:
            if error.code != HTTPStatus.UNAUTHORIZED:
                raise MirrorTestError(f"HTTP {error.code} from {url}") from error
            auth_info = self._parse_www_authenticate(error.headers.get("WWW-Authenticate"))
            if not auth_info:
                raise MirrorTestError("registry requires auth but sent no Bearer challenge") from error
            error.close()
            token = self._get_token(auth_info, scope_override=scope)
            retry_headers = dict(headers)
            retry_headers["Authorization"] = f"Bearer {token}"
            return self._open(url, headers=retry_headers, timeout=timeout)
        except urllib.error.URLError as error:
            raise MirrorTestError(f"request failed: {error}") from error

    def _parse_image(self, image):
        name, _, reference = image.partition(":")
        if not name or not reference:
            raise MirrorTestError("image must look like library/ubuntu:latest")
        return name, reference

    def _manifest_url(self, repository, reference):
        return f"{self.mirror}/v2/{repository}/manifests/{reference}"

    def _blob_url(self, repository, digest):
        return f"{self.mirror}/v2/{repository}/blobs/{digest}"

    def _tags_url(self, repository, limit=200):
        return f"{self.mirror}/v2/{repository}/tags/list?n={int(limit)}"

    def _pick_fallback_tag(self, tags):
        if not tags:
            raise MirrorTestError("registry tags list is empty")
        names = [str(tag).strip() for tag in tags if str(tag).strip()]
        if not names:
            raise MirrorTestError("registry tags list is empty")

        preferred = ["latest", "stable", "buildx-stable-1", "master", "main"]
        lowered = {name.lower(): name for name in names}
        for key in preferred:
            if key in lowered:
                return lowered[key]

        semver_like = [
            name
            for name in names
            if re.match(r"^v?\d+(?:\.\d+){1,3}(?:[-+._][A-Za-z0-9.-]+)?$", name)
        ]
        if semver_like:
            return max(semver_like, key=natural_version_key)
        return max(names, key=natural_version_key)

    def resolve_manifest(self, image, platform_os="linux", platform_arch="amd64"):
        repository, reference = self._parse_image(image)
        scope = f"repository:{repository}:pull"
        selected_reference = reference
        try:
            manifest, headers = self._request_json(
                self._manifest_url(repository, selected_reference),
                headers={"Accept": MANIFEST_ACCEPT},
                scope=scope,
            )
        except MirrorTestError as error:
            # Some registries don't publish "latest". Fall back to an available tag.
            if reference.lower() != "latest" or "HTTP 404" not in str(error):
                raise
            tags_payload, _ = self._request_json(self._tags_url(repository), scope=scope)
            tags = tags_payload.get("tags") or []
            selected_reference = self._pick_fallback_tag(tags)
            manifest, headers = self._request_json(
                self._manifest_url(repository, selected_reference),
                headers={"Accept": MANIFEST_ACCEPT},
                scope=scope,
            )
        media_type = manifest.get("mediaType") or headers.get("Content-Type", "")
        if "manifest.list" in media_type or "image.index" in media_type:
            manifests = manifest.get("manifests", [])
            selected = None
            for item in manifests:
                platform = item.get("platform") or {}
                if platform.get("os") == platform_os and platform.get("architecture") == platform_arch:
                    selected = item
                    break
            if selected is None and manifests:
                selected = manifests[0]
            if selected is None:
                raise MirrorTestError("manifest list returned no platform entries")
            manifest, headers = self._request_json(
                self._manifest_url(repository, selected["digest"]),
                headers={"Accept": MANIFEST_ACCEPT},
                scope=scope,
            )
            media_type = manifest.get("mediaType") or headers.get("Content-Type", media_type)
        layers = manifest.get("layers") or []
        if not layers:
            raise MirrorTestError("resolved manifest has no layers")
        return {
            "repository": repository,
            "reference": selected_reference,
            "media_type": media_type,
            "layers": layers,
        }

    def select_layer(self, layers, target_bytes):
        sized_layers = [layer for layer in layers if isinstance(layer.get("size"), int)]
        if not sized_layers:
            raise MirrorTestError("manifest layers have no size info")
        within_window = [
            layer for layer in sized_layers if target_bytes <= layer["size"] <= target_bytes * 8
        ]
        if within_window:
            return min(within_window, key=lambda layer: abs(layer["size"] - target_bytes))
        larger = [layer for layer in sized_layers if layer["size"] >= target_bytes]
        if larger:
            return min(larger, key=lambda layer: layer["size"])
        return max(sized_layers, key=lambda layer: layer["size"])

    def download_sample(self, repository, digest, sample_bytes):
        scope = f"repository:{repository}:pull"
        headers = {"Range": f"bytes=0-{sample_bytes - 1}"}
        start = time.perf_counter()
        deadline = start + self.max_duration
        first_byte_at = None
        total_bytes = 0
        status = None
        response_headers = {}
        final_url = self._blob_url(repository, digest)
        try:
            with self._request_response(
                final_url,
                headers=headers,
                scope=scope,
                timeout=self.start_timeout,
            ) as response:
                status = response.status
                response_headers = dict(response.headers)
                final_url = response.geturl()
                while total_bytes < sample_bytes:
                    remaining = deadline - time.perf_counter()
                    if remaining <= 0:
                        break
                    if first_byte_at is not None:
                        _set_response_socket_timeout(response, min(self.timeout, max(0.5, remaining)))
                    chunk_size = min(64 * 1024, sample_bytes - total_bytes)
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    if first_byte_at is None:
                        first_byte_at = time.perf_counter()
                        _set_response_socket_timeout(
                            response,
                            min(self.timeout, max(0.5, deadline - first_byte_at)),
                        )
                    total_bytes += len(chunk)
        except MirrorTestError:
            raise
        except (OSError, TimeoutError) as error:
            if first_byte_at is None:
                raise MirrorTestError(f"download start timeout (>{self.start_timeout}s): {error}") from error
            raise MirrorTestError(f"download interrupted: {error}") from error
        finished_at = time.perf_counter()
        if first_byte_at is None and total_bytes <= 0:
            raise MirrorTestError(f"download start timeout (>{self.start_timeout}s): no data received")
        total_time = finished_at - start
        first_byte_time = (first_byte_at - start) if first_byte_at is not None else total_time
        speed_bps = total_bytes / total_time if total_time > 0 else 0.0
        return {
            "status": status,
            "url": final_url,
            "bytes_downloaded": total_bytes,
            "time_total_s": round(total_time, 3),
            "time_to_first_byte_s": round(first_byte_time, 3),
            "speed_bps": speed_bps,
            "speed_mbps": round(speed_bps / 1024 / 1024, 2),
            "content_length": response_headers.get("Content-Length"),
            "content_range": response_headers.get("Content-Range"),
            "accept_ranges": response_headers.get("Accept-Ranges"),
            "time_capped": total_bytes < sample_bytes,
            "location": response_headers.get("Location"),
        }


class SimpleIndexParser(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)


def parse_pip_target(target):
    match = re.match(r"^\s*([A-Za-z0-9_.-]+)\s*(?:==\s*([A-Za-z0-9_.!+\-]+))?\s*$", str(target))
    if not match:
        raise MirrorTestError("pip 包格式应为 package 或 package==version")
    package = match.group(1)
    version = match.group(2)
    normalized = re.sub(r"[-_.]+", "-", package).lower()
    return normalized, version


def choose_pip_artifact(index_url, links, version=None):
    candidates = []
    for href in links:
        full_url = urllib.parse.urldefrag(urllib.parse.urljoin(index_url, href))[0]
        filename = filename_from_url(full_url)
        lower = filename.lower()
        if lower.endswith(".whl"):
            score = 30
        elif lower.endswith(".tar.gz"):
            score = 20
        elif lower.endswith(".zip") or lower.endswith(".tgz"):
            score = 10
        else:
            continue
        if version:
            score += 40 if version.lower() in lower else -20
        if "py3-none-any" in lower:
            score += 6
        if "manylinux" in lower:
            score += 4
        if "macos" in lower or "win_" in lower:
            score -= 2
        candidates.append((score, filename, full_url))
    if not candidates:
        raise MirrorTestError("simple index contains no downloadable artifacts")
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    _, filename, artifact_url = candidates[0]
    return {
        "filename": filename,
        "url": artifact_url,
    }


def parse_maven_target(target):
    value = str(target).strip()
    parts = [part.strip() for part in value.split(":")]
    if len(parts) < 3:
        raise MirrorTestError("Maven 坐标格式应为 groupId:artifactId:version")
    group_id, artifact_id, version = parts[:3]
    packaging = parts[3] if len(parts) >= 4 and parts[3] else "jar"
    classifier = parts[4] if len(parts) >= 5 and parts[4] else ""
    if not group_id or not artifact_id or not version:
        raise MirrorTestError("Maven 坐标格式应为 groupId:artifactId:version")
    return {
        "group_id": group_id,
        "artifact_id": artifact_id,
        "version": version,
        "packaging": packaging,
        "classifier": classifier,
    }


def build_maven_artifact_url(mirror, target):
    coord = parse_maven_target(target)
    group_path = coord["group_id"].replace(".", "/")
    classifier = f"-{coord['classifier']}" if coord["classifier"] else ""
    filename = f"{coord['artifact_id']}-{coord['version']}{classifier}.{coord['packaging']}"
    rel_path = (
        f"{group_path}/{coord['artifact_id']}/{coord['version']}/{filename}"
    )
    artifact_url = urllib.parse.urljoin(f"{mirror.rstrip('/')}/", rel_path)
    return coord, artifact_url


def go_proxy_escape(value):
    escaped = []
    for ch in str(value):
        if "A" <= ch <= "Z":
            escaped.append("!" + ch.lower())
        else:
            escaped.append(ch)
    return urllib.parse.quote("".join(escaped), safe="/-._~!+")


def parse_go_target(target):
    value = str(target).strip()
    if not value:
        raise MirrorTestError("Go 目标不能为空")
    module, sep, version = value.partition("@")
    module = module.strip().strip("/")
    if not module:
        raise MirrorTestError("Go 目标格式应为 module@version")
    if not sep:
        version = "latest"
    version = version.strip() or "latest"
    return module, version


def resolve_go_version(mirror, module, version):
    if version != "latest":
        return version
    module_path = go_proxy_escape(module)
    latest_url = f"{mirror.rstrip('/')}/{module_path}/@latest"
    payload = fetch_json(latest_url)
    resolved = payload.get("Version")
    if not resolved:
        raise MirrorTestError(f"go proxy latest response has no Version: {latest_url}")
    return str(resolved)


def parse_cargo_target(target):
    value = str(target).strip()
    if not value:
        raise MirrorTestError("Cargo 目标不能为空")
    crate, sep, version = value.partition("@")
    crate = crate.strip()
    if not crate or not re.match(r"^[A-Za-z0-9_-]+$", crate):
        raise MirrorTestError("Cargo 目标格式应为 crate@version，crate 仅支持字母数字_-")
    crate = crate.lower()
    if not sep:
        version = "latest"
    version = version.strip() or "latest"
    return crate, version


def resolve_cargo_version(mirror, crate, version):
    if version != "latest":
        return version
    meta_url = f"{mirror.rstrip('/')}/api/v1/crates/{urllib.parse.quote(crate)}"
    try:
        payload = fetch_json(meta_url)
        crate_meta = payload.get("crate") or {}
        resolved = crate_meta.get("max_version")
        if resolved:
            return str(resolved)
    except MirrorTestError:
        pass

    index_url = f"{mirror.rstrip('/')}/index/{cargo_index_rel_path(crate)}"
    text = fetch_text(index_url)
    latest_non_yanked = None
    latest_any = None
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        row_ver = str(row.get("vers") or "").strip()
        if not row_ver:
            continue
        latest_any = row_ver
        if not row.get("yanked"):
            latest_non_yanked = row_ver
    resolved = latest_non_yanked or latest_any
    if not resolved:
        raise MirrorTestError(f"cargo index has no valid versions: {index_url}")
    return resolved


def cargo_index_rel_path(crate):
    name = str(crate).strip().lower()
    length = len(name)
    if length == 1:
        return f"1/{name}"
    if length == 2:
        return f"2/{name}"
    if length == 3:
        return f"3/{name[0]}/{name}"
    return f"{name[:2]}/{name[2:4]}/{name}"


def resolve_cargo_download_url(mirror, crate, version):
    crate_q = urllib.parse.quote(crate)
    ver_q = urllib.parse.quote(version)
    default_base = f"{mirror.rstrip('/')}/api/v1/crates"
    dl_template = None
    try:
        config = fetch_json(f"{mirror.rstrip('/')}/index/config.json")
        dl_template = str(config.get("dl") or "").strip() or None
    except MirrorTestError:
        dl_template = None

    if dl_template and ("{crate}" in dl_template or "{version}" in dl_template):
        return dl_template.replace("{crate}", crate_q).replace("{version}", ver_q)

    dl_base = dl_template or default_base
    return f"{dl_base.rstrip('/')}/{crate_q}/{ver_q}/download"


def parse_nuget_target(target):
    value = str(target).strip()
    if not value:
        raise MirrorTestError("NuGet 目标不能为空")
    package, sep, version = value.partition("@")
    package = package.strip()
    if not package:
        raise MirrorTestError("NuGet 目标格式应为 package 或 package@version")
    if not sep:
        version = "latest"
    version = version.strip() or "latest"
    return package, version


def normalize_nuget_index_url(mirror):
    value = str(mirror).strip().rstrip("/")
    if value.endswith("/v3/index.json") or value.endswith(".json"):
        return value
    if value.endswith("/v3"):
        return f"{value}/index.json"
    return f"{value}/v3/index.json"


def resolve_nuget_download_url(mirror, package, version):
    package_lower = package.lower()
    index_url = normalize_nuget_index_url(mirror)
    index_payload = fetch_json(index_url)
    package_base_url = None
    for item in index_payload.get("resources") or []:
        if not isinstance(item, dict):
            continue
        res_type = str(item.get("@type") or "")
        if res_type.startswith("PackageBaseAddress"):
            package_base_url = str(item.get("@id") or "").rstrip("/") + "/"
            break
    if not package_base_url:
        raise MirrorTestError(f"NuGet index missing PackageBaseAddress: {index_url}")

    versions_url = f"{package_base_url}{urllib.parse.quote(package_lower)}/index.json"
    versions_payload = fetch_json(versions_url)
    versions = [str(v).strip() for v in (versions_payload.get("versions") or []) if str(v).strip()]
    if not versions:
        raise MirrorTestError(f"NuGet package has no versions: {package}")

    if version != "latest":
        resolved_version = version
    else:
        stable_versions = [v for v in versions if "-" not in v]
        pool = stable_versions or versions
        resolved_version = max(pool, key=natural_version_key)
    version_lower = resolved_version.lower()
    nupkg_url = (
        f"{package_base_url}{urllib.parse.quote(package_lower)}/"
        f"{urllib.parse.quote(version_lower)}/"
        f"{urllib.parse.quote(package_lower)}.{urllib.parse.quote(version_lower)}.nupkg"
    )
    return {
        "index_url": index_url,
        "version": resolved_version,
        "url": nupkg_url,
    }


def parse_conda_target(target):
    match = re.match(r"^\s*([A-Za-z0-9_.-]+)\s*(?:==\s*([A-Za-z0-9_.!+\-]+))?\s*$", str(target))
    if not match:
        raise MirrorTestError("Conda 包格式应为 package 或 package==version")
    package = match.group(1).strip().lower()
    version = (match.group(2) or "").strip()
    return package, (version or "latest")


def resolve_conda_artifact(mirror, package, version):
    base = str(mirror).strip().rstrip("/")
    repodata = None
    repodata_url = None
    for name in ("current_repodata.json", "repodata.json"):
        candidate = f"{base}/pkgs/main/linux-64/{name}"
        try:
            repodata = fetch_json(candidate, timeout=max(DEFAULT_TIMEOUT, 30))
            repodata_url = candidate
            break
        except MirrorTestError:
            continue
    if repodata is None:
        raise MirrorTestError(f"Conda repodata not available: {base}")

    candidates = []
    for section_name, section in (
        ("packages.conda", repodata.get("packages.conda") or {}),
        ("packages", repodata.get("packages") or {}),
    ):
        for filename, meta in section.items():
            if not isinstance(meta, dict):
                continue
            if str(meta.get("name") or "").strip().lower() != package:
                continue
            row_version = str(meta.get("version") or "").strip()
            if not row_version:
                continue
            if version != "latest" and row_version != version:
                continue
            build_no = meta.get("build_number")
            build_no = build_no if isinstance(build_no, int) else 0
            ext_rank = 1 if str(filename).endswith(".conda") else 0
            candidates.append(
                (
                    natural_version_key(row_version),
                    build_no,
                    ext_rank,
                    str(filename),
                    row_version,
                    section_name,
                )
            )
    if not candidates:
        raise MirrorTestError(f"Conda package not found: {package} ({version})")

    _, _, _, filename, resolved_version, source_section = max(candidates)
    artifact_url = f"{base}/pkgs/main/linux-64/{urllib.parse.quote(filename)}"
    return {
        "repodata_url": repodata_url,
        "filename": filename,
        "version": resolved_version,
        "source": source_section,
        "url": artifact_url,
    }


def parse_git_target(target):
    value = str(target).strip()
    if not value:
        raise MirrorTestError("Git 仓库目标不能为空")
    if is_http_url(value):
        parsed = urllib.parse.urlparse(value)
        repo = parsed.path.strip("/")
    else:
        repo = value.strip().lstrip("/")
    if repo.endswith(".git"):
        repo = repo[:-4]
    if repo.count("/") < 1:
        raise MirrorTestError("Git 目标格式应为 owner/repo 或完整仓库 URL")
    return repo


def build_git_repo_url(mirror, repo):
    mirror = str(mirror).strip().rstrip("/")
    repo = str(repo).strip().strip("/")
    name = repo.rsplit("/", 1)[-1].replace(".git", "")
    if "{repo}" in mirror:
        url = mirror.replace("{repo}", repo)
    elif "{name}" in mirror:
        url = mirror.replace("{name}", name)
    elif "{path}" in mirror:
        url = mirror.replace("{path}", repo)
    elif "{url}" in mirror:
        url = mirror.replace("{url}", f"https://github.com/{repo}")
    else:
        url = urllib.parse.urljoin(f"{mirror}/", repo)
    return url if url.endswith(".git") else f"{url}.git"


def ping_stream_url(url, timeout=DEFAULT_START_TIMEOUT):
    start = time.perf_counter()
    try:
        with open_url(url, method="GET", timeout=timeout) as response:
            response.read(1)
            elapsed = time.perf_counter() - start
            return {
                "ok": True,
                "status": response.status,
                "latency_ms": round(elapsed * 1000, 1),
            }
    except urllib.error.HTTPError as error:
        raise MirrorTestError(f"ping failed: HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise MirrorTestError(f"ping failed: {error}") from error


def test_path_mirror(kind, mirror, target, sample_bytes, allow_full_url_prefix=False, probe_only=False):
    result = make_base_result(kind, mirror, target, sample_bytes)
    try:
        target_url = render_mirror_target(
            mirror,
            target,
            allow_full_url_prefix=allow_full_url_prefix,
        )
        result["ping"] = ping_url(target_url)
        result["subject"] = {
            "label": filename_from_url(target_url),
            "detail": target_url,
        }
        if probe_only:
            result["ok"] = True
            result["probe_only"] = True
            return result
        result["download"] = download_sample_url(target_url, sample_bytes)
        result["ok"] = True
    except Exception as error:
        result["ok"] = False
        result["error"] = str(error)
    return result


def test_docker_mirror(mirror, target, sample_bytes, probe_only=False):
    client = RegistryClient(mirror)
    result = make_base_result("docker", mirror, target, sample_bytes)
    try:
        result["ping"] = client.ping()
        manifest = client.resolve_manifest(target)
        layer = client.select_layer(manifest["layers"], sample_bytes)
        result["manifest"] = {
            "repository": manifest["repository"],
            "reference": manifest["reference"],
            "media_type": manifest["media_type"],
            "layer_count": len(manifest["layers"]),
        }
        result["subject"] = {
            "label": layer["digest"],
            "detail": layer.get("mediaType") or "layer",
            "size": layer["size"],
        }
        if probe_only:
            result["ok"] = True
            result["probe_only"] = True
            return result
        result["download"] = client.download_sample(
            manifest["repository"],
            layer["digest"],
            sample_bytes,
        )
        result["ok"] = True
    except Exception as error:
        result["ok"] = False
        result["error"] = str(error)
    return result


def test_github_mirror(mirror, target, sample_bytes, probe_only=False):
    return test_path_mirror(
        "github",
        mirror,
        target,
        sample_bytes,
        allow_full_url_prefix=True,
        probe_only=probe_only,
    )


def test_generic_mirror(mirror, target, sample_bytes, probe_only=False):
    return test_path_mirror("generic", mirror, target, sample_bytes, probe_only=probe_only)


def test_pip_mirror(mirror, target, sample_bytes, probe_only=False):
    result = make_base_result("pip", mirror, target, sample_bytes)
    try:
        package, version = parse_pip_target(target)
        index_root = render_pip_index_url(mirror)
        if not index_root:
            raise MirrorTestError("pip 镜像地址不能为空")
        index_url = urllib.parse.urljoin(f"{index_root.rstrip('/')}/", f"{urllib.parse.quote(package)}/")
        result["ping"] = ping_url(index_url)
        parser = SimpleIndexParser()
        parser.feed(fetch_text(index_url))
        artifact = choose_pip_artifact(index_url, parser.links, version=version)
        result["subject"] = {
            "label": artifact["filename"],
            "detail": artifact["url"],
        }
        if probe_only:
            result["ok"] = True
            result["probe_only"] = True
            return result
        result["download"] = download_sample_url(artifact["url"], sample_bytes)
        result["ok"] = True
    except Exception as error:
        result["ok"] = False
        result["error"] = str(error)
    return result


def test_npm_mirror(mirror, target, sample_bytes, probe_only=False):
    result = make_base_result("npm", mirror, target, sample_bytes)
    try:
        package = str(target).strip().lstrip("/")
        pkg_url = f"{mirror.rstrip('/')}/{urllib.parse.quote(package, safe='@/')}"
        pkg_text, latency_ms = fetch_text_with_latency(pkg_url)
        result["ping"] = {"ok": True, "status": 200, "latency_ms": latency_ms}
        pkg_meta = json.loads(pkg_text)
        dist_tags = pkg_meta.get("dist-tags") or {}
        latest_version = dist_tags.get("latest")
        if not latest_version:
            versions = pkg_meta.get("versions") or {}
            if not versions:
                raise MirrorTestError("package metadata has no versions")
            latest_version = list(versions.keys())[-1]
        versions = pkg_meta.get("versions") or {}
        version_info = versions.get(latest_version)
        if not version_info:
            raise MirrorTestError(f"version {latest_version} not found in metadata")
        tarball_url = (version_info.get("dist") or {}).get("tarball")
        if not tarball_url:
            raise MirrorTestError("no tarball URL in version dist info")
        # Replace the registry domain in tarball URL with the mirror domain
        tarball_parsed = urllib.parse.urlparse(tarball_url)
        mirror_parsed = urllib.parse.urlparse(mirror.rstrip("/"))
        tarball_url = urllib.parse.urlunparse(
            (mirror_parsed.scheme, mirror_parsed.netloc,
             tarball_parsed.path, tarball_parsed.params,
             tarball_parsed.query, tarball_parsed.fragment)
        )
        result["subject"] = {
            "label": f"{package}@{latest_version}",
            "detail": tarball_url,
        }
        if probe_only:
            result["ok"] = True
            result["probe_only"] = True
            return result
        result["download"] = download_sample_url(tarball_url, sample_bytes)
        result["ok"] = True
    except Exception as error:
        result["ok"] = False
        result["error"] = str(error)
    return result


def test_maven_mirror(mirror, target, sample_bytes, probe_only=False):
    result = make_base_result("maven", mirror, target, sample_bytes)
    try:
        coord, artifact_url = build_maven_artifact_url(mirror, target)
        result["ping"] = ping_url(artifact_url)
        label = f"{coord['group_id']}:{coord['artifact_id']}:{coord['version']}"
        result["subject"] = {
            "label": label,
            "detail": artifact_url,
        }
        if probe_only:
            result["ok"] = True
            result["probe_only"] = True
            return result
        result["download"] = download_sample_url(artifact_url, sample_bytes)
        result["ok"] = True
    except Exception as error:
        result["ok"] = False
        result["error"] = str(error)
    return result


def test_go_mirror(mirror, target, sample_bytes, probe_only=False):
    result = make_base_result("go", mirror, target, sample_bytes)
    try:
        module, version = parse_go_target(target)
        resolved_version = resolve_go_version(mirror, module, version)
        module_path = go_proxy_escape(module)
        version_path = go_proxy_escape(resolved_version)
        zip_url = f"{mirror.rstrip('/')}/{module_path}/@v/{version_path}.zip"
        result["ping"] = ping_url(zip_url)
        result["subject"] = {
            "label": f"{module}@{resolved_version}",
            "detail": zip_url,
        }
        if probe_only:
            result["ok"] = True
            result["probe_only"] = True
            return result
        result["download"] = download_sample_url(zip_url, sample_bytes)
        result["ok"] = True
    except Exception as error:
        result["ok"] = False
        result["error"] = str(error)
    return result


def test_cargo_mirror(mirror, target, sample_bytes, probe_only=False):
    result = make_base_result("cargo", mirror, target, sample_bytes)
    try:
        crate, version = parse_cargo_target(target)
        resolved_version = resolve_cargo_version(mirror, crate, version)
        download_url = resolve_cargo_download_url(mirror, crate, resolved_version)
        result["ping"] = ping_url(download_url)
        result["subject"] = {
            "label": f"{crate}@{resolved_version}",
            "detail": download_url,
        }
        if probe_only:
            result["ok"] = True
            result["probe_only"] = True
            return result
        result["download"] = download_sample_url(download_url, sample_bytes)
        result["ok"] = True
    except Exception as error:
        result["ok"] = False
        result["error"] = str(error)
    return result


def test_nuget_mirror(mirror, target, sample_bytes, probe_only=False):
    result = make_base_result("nuget", mirror, target, sample_bytes)
    try:
        package, version = parse_nuget_target(target)
        resolved = resolve_nuget_download_url(mirror, package, version)
        result["ping"] = ping_url(resolved["url"])
        result["subject"] = {
            "label": f"{package}@{resolved['version']}",
            "detail": resolved["url"],
        }
        if probe_only:
            result["ok"] = True
            result["probe_only"] = True
            return result
        result["download"] = download_sample_url(resolved["url"], sample_bytes)
        result["ok"] = True
    except Exception as error:
        result["ok"] = False
        result["error"] = str(error)
    return result


def test_conda_mirror(mirror, target, sample_bytes, probe_only=False):
    result = make_base_result("conda", mirror, target, sample_bytes)
    try:
        package, version = parse_conda_target(target)
        artifact = resolve_conda_artifact(mirror, package, version)
        result["ping"] = ping_url(artifact["url"])
        result["subject"] = {
            "label": f"{package}=={artifact['version']}",
            "detail": artifact["url"],
        }
        if probe_only:
            result["ok"] = True
            result["probe_only"] = True
            return result
        result["download"] = download_sample_url(artifact["url"], sample_bytes)
        result["ok"] = True
    except Exception as error:
        result["ok"] = False
        result["error"] = str(error)
    return result


def test_homebrew_mirror(mirror, target, sample_bytes, probe_only=False):
    return test_path_mirror("homebrew", mirror, target, sample_bytes, probe_only=probe_only)


def test_git_mirror(mirror, target, sample_bytes, probe_only=False):
    result = make_base_result("git", mirror, target, sample_bytes)
    try:
        repo = parse_git_target(target)
        clone_url = build_git_repo_url(mirror, repo)
        info_refs_url = f"{clone_url.rstrip('/')}/info/refs?service=git-upload-pack"
        result["ping"] = ping_stream_url(info_refs_url)
        result["subject"] = {
            "label": repo,
            "detail": clone_url,
        }
        if probe_only:
            result["ok"] = True
            result["probe_only"] = True
            return result
        result["download"] = download_sample_url(info_refs_url, sample_bytes)
        result["ok"] = True
    except Exception as error:
        result["ok"] = False
        result["error"] = str(error)
    return result


def test_apt_mirror(mirror, target, sample_bytes, probe_only=False):
    return test_path_mirror("apt", mirror, target, sample_bytes, probe_only=probe_only)


def test_yum_mirror(mirror, target, sample_bytes, probe_only=False):
    return test_path_mirror("yum", mirror, target, sample_bytes, probe_only=probe_only)


def test_apk_mirror(mirror, target, sample_bytes, probe_only=False):
    return test_path_mirror("apk", mirror, target, sample_bytes, probe_only=probe_only)


def test_flatpak_mirror(mirror, target, sample_bytes, probe_only=False):
    return test_path_mirror("flatpak", mirror, target, sample_bytes, probe_only=probe_only)


def default_target_for_kind(kind):
    if kind == "docker":
        return DEFAULT_DOCKER_TARGET
    if kind == "github":
        return DEFAULT_GITHUB_TARGET
    if kind == "generic":
        return DEFAULT_GENERIC_TARGET
    if kind == "pip":
        return DEFAULT_PIP_TARGET
    if kind == "npm":
        return DEFAULT_NPM_TARGET
    if kind == "maven":
        return DEFAULT_MAVEN_TARGET
    if kind == "go":
        return DEFAULT_GO_TARGET
    if kind == "cargo":
        return DEFAULT_CARGO_TARGET
    if kind == "nuget":
        return DEFAULT_NUGET_TARGET
    if kind == "conda":
        return DEFAULT_CONDA_TARGET
    if kind == "homebrew":
        return DEFAULT_HOMEBREW_TARGET
    if kind == "git":
        return DEFAULT_GIT_TARGET
    if kind == "apt":
        return DEFAULT_APT_TARGET
    if kind == "yum":
        return DEFAULT_YUM_TARGET
    if kind == "apk":
        return DEFAULT_APK_TARGET
    if kind == "flatpak":
        return DEFAULT_FLATPAK_TARGET
    raise MirrorTestError(f"unsupported kind: {kind}")


TESTERS = {
    "docker": test_docker_mirror,
    "github": test_github_mirror,
    "generic": test_generic_mirror,
    "pip": test_pip_mirror,
    "npm": test_npm_mirror,
    "maven": test_maven_mirror,
    "go": test_go_mirror,
    "cargo": test_cargo_mirror,
    "nuget": test_nuget_mirror,
    "conda": test_conda_mirror,
    "homebrew": test_homebrew_mirror,
    "git": test_git_mirror,
    "apt": test_apt_mirror,
    "yum": test_yum_mirror,
    "apk": test_apk_mirror,
    "flatpak": test_flatpak_mirror,
}

DEFAULT_MIRROR_CONFIG = {
    "docker": {
        "label": "Docker 镜像源",
        "icon": "\U0001f433",
        "icon_url": "https://cdn.simpleicons.org/docker/2496ED",
        "tip": "默认目标：homebrew/brew:latest。部分镜像源支持多目标仓库直拉，如 dockerproxy.cool、docker-pull.ygxz.in、docker.1ms.run；ghcr.nju.edu.cn 仅适用于 GHCR。",
        "mirrors": [
            "https://registry-1.docker.io",
            "https://dockerproxy.cool",
            "https://2a6bf1988cb6428c877f723ec7530dbc.mirror.swr.myhuaweicloud.com",
            "https://docker-pull.ygxz.in",
            "https://docker.etcd.fun",
            "https://docker.1ms.run",
            "https://d.yydy.link:2023",
            "https://hub.mirrorify.net",
            "https://docker.kejilion.pro",
            "https://ghcr.nju.edu.cn",
        ],
        "target": DEFAULT_DOCKER_TARGET,
        "sample_mb": 10,
    },
    "github": {
        "label": "GitHub 代理",
        "icon": "\U0001f4e6",
        "icon_url": "https://cdn.simpleicons.org/github/181717",
        "mirrors": [
            "https://github.com",
            "https://cors.isteed.cc/{url}",
            "https://gh-proxy.com/{url}",
            "https://gh.xxooo.cf/{url}",
            "https://ghfast.top/{url}",
            "https://wget.la/{url}",
            "https://ghproxy.net/{url}",
        ],
        "target": DEFAULT_GITHUB_TARGET,
        "sample_mb": 10,
    },
    "generic": {
        "label": "自定义源",
        "icon": "\U0001f310",
        "icon_url": "",
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn",
            "https://mirrors.ustc.edu.cn",
        ],
        "target": DEFAULT_GENERIC_TARGET,
        "sample_mb": 10,
    },
    "pip": {
        "label": "pip 镜像",
        "icon": "\U0001f40d",
        "icon_url": "https://cdn.simpleicons.org/pypi/3775A9",
        "mirrors": [
            "https://pypi.tuna.tsinghua.edu.cn",
            "https://mirrors.aliyun.com/pypi",
            "https://repo.huaweicloud.com/repository/pypi",
        ],
        "target": DEFAULT_PIP_TARGET,
        "sample_mb": 5,
    },
    "npm": {
        "label": "npm 镜像",
        "icon": "\U0001f4e6",
        "icon_url": "https://cdn.simpleicons.org/npm/CB3837",
        "mirrors": [
            "https://registry.npmmirror.com",
            "https://repo.huaweicloud.com/repository/npm",
            "https://mirrors.cloud.tencent.com/npm",
        ],
        "target": DEFAULT_NPM_TARGET,
        "sample_mb": 5,
    },
    "maven": {
        "label": "Maven / Gradle",
        "icon": "\u2615",
        "icon_url": "https://cdn.simpleicons.org/apachemaven/C71A36",
        "mirrors": [
            "https://repo1.maven.org/maven2",
            "https://maven.aliyun.com/repository/public",
            "https://repo.huaweicloud.com/repository/maven",
            "https://mirrors.tencent.com/nexus/repository/maven-public",
        ],
        "target": DEFAULT_MAVEN_TARGET,
        "sample_mb": 5,
    },
    "go": {
        "label": "Go Proxy",
        "icon": "\U0001f439",
        "icon_url": "https://cdn.simpleicons.org/go/00ADD8",
        "mirrors": [
            "https://goproxy.cn",
            "https://goproxy.io",
            "https://mirrors.aliyun.com/goproxy/",
            "https://proxy.golang.com.cn",
        ],
        "target": DEFAULT_GO_TARGET,
        "sample_mb": 5,
    },
    "cargo": {
        "label": "Cargo (crates)",
        "icon": "\U0001f980",
        "icon_url": "https://cdn.simpleicons.org/rust/000000",
        "mirrors": [
            "https://rsproxy.cn",
            "https://crates.io",
        ],
        "target": DEFAULT_CARGO_TARGET,
        "sample_mb": 5,
    },
    "nuget": {
        "label": "NuGet",
        "icon": "\U0001f7e3",
        "icon_url": "https://cdn.simpleicons.org/nuget/004880",
        "mirrors": [
            "https://repo.huaweicloud.com/repository/nuget",
            "https://nuget.azure.cn",
            "https://api.nuget.org",
        ],
        "target": DEFAULT_NUGET_TARGET,
        "sample_mb": 5,
    },
    "conda": {
        "label": "Conda",
        "icon": "\U0001f9ea",
        "icon_url": "https://cdn.simpleicons.org/anaconda/44A833",
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/anaconda",
            "https://mirrors.ustc.edu.cn/anaconda",
            "https://mirrors.bfsu.edu.cn/anaconda",
            "https://repo.anaconda.com",
        ],
        "target": DEFAULT_CONDA_TARGET,
        "sample_mb": 5,
    },
    "homebrew": {
        "label": "Homebrew",
        "icon": "\U0001f37a",
        "icon_url": "https://cdn.simpleicons.org/homebrew/FBB040",
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn/homebrew-bottles",
            "https://mirrors.ustc.edu.cn/homebrew-bottles",
            "https://formulae.brew.sh",
        ],
        "target": DEFAULT_HOMEBREW_TARGET,
        "sample_mb": 5,
    },
    "git": {
        "label": "Git Clone",
        "icon": "\U0001f5c3",
        "icon_url": "https://cdn.simpleicons.org/git/F05032",
        "mirrors": [
            "https://gitclone.com/github.com/{repo}",
            "https://gitee.com/mirrors/{name}",
            "https://github.com/{repo}",
        ],
        "target": DEFAULT_GIT_TARGET,
        "sample_mb": 1,
    },
    "apt": {
        "label": "APT (Debian/Ubuntu)",
        "icon": "\U0001f9ca",
        "icon_url": "https://cdn.simpleicons.org/debian/A81D33",
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn",
            "https://mirrors.ustc.edu.cn",
            "https://repo.huaweicloud.com",
            "https://mirrors.cloud.tencent.com",
        ],
        "target": DEFAULT_APT_TARGET,
        "sample_mb": 5,
    },
    "yum": {
        "label": "YUM / DNF (CentOS/RHEL)",
        "icon": "\U0001f4be",
        "icon_url": "https://cdn.simpleicons.org/centos/932279",
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn",
            "https://mirrors.ustc.edu.cn",
            "https://repo.huaweicloud.com",
            "https://mirrors.aliyun.com",
        ],
        "target": DEFAULT_YUM_TARGET,
        "sample_mb": 2,
    },
    "apk": {
        "label": "APK (Alpine)",
        "icon": "\U0001f433",
        "icon_url": "https://cdn.simpleicons.org/alpinelinux/0D597F",
        "mirrors": [
            "https://mirrors.tuna.tsinghua.edu.cn",
            "https://mirrors.ustc.edu.cn",
            "https://repo.huaweicloud.com",
            "https://mirrors.aliyun.com",
        ],
        "target": DEFAULT_APK_TARGET,
        "sample_mb": 2,
    },
    "flatpak": {
        "label": "Flatpak (Flathub)",
        "icon": "\U0001f9ca",
        "icon_url": "https://cdn.simpleicons.org/flatpak/4A90D9",
        "mirrors": [
            "https://dl.flathub.org",
            "https://flathub.org",
        ],
        "target": DEFAULT_FLATPAK_TARGET,
        "sample_mb": 5,
    },
}


def dedupe_mirrors(mirrors):
    unique_mirrors = []
    seen = set()
    for mirror in mirrors:
        value = str(mirror).strip().rstrip("/")
        if not value or value in seen:
            continue
        seen.add(value)
        unique_mirrors.append(value)
    return unique_mirrors


def normalize_pip_mirror_base(mirror):
    value = str(mirror or "").strip().rstrip("/")
    if value.lower().endswith("/simple"):
        value = value[: -len("/simple")].rstrip("/")
    return value


def render_pip_index_url(mirror):
    base = normalize_pip_mirror_base(mirror)
    if not base:
        return ""
    return f"{base}/simple"


def deep_copy_json(value):
    return json.loads(json.dumps(value, ensure_ascii=False))


def get_runtime_config_snapshot():
    return deep_copy_json(RUNTIME_MIRROR_CONFIG)


RUNTIME_MIRROR_CONFIG = deep_copy_json(DEFAULT_MIRROR_CONFIG)


def normalize_test_request(kind=None, mirrors=None, target=None, sample_mb=None, probe_only=None):
    normalized_kind = str(kind or DEFAULT_KIND).strip().lower()
    if normalized_kind not in TESTERS:
        raise ValueError(f"unsupported kind: {normalized_kind}")

    kind_defaults = deep_copy_json(RUNTIME_MIRROR_CONFIG.get(normalized_kind, {}))
    if mirrors is not None:
        input_mirrors = mirrors
    else:
        disabled_set = set(dedupe_mirrors(kind_defaults.get("disabled_mirrors", [])))
        input_mirrors = [
            mirror
            for mirror in (kind_defaults.get("mirrors") or [])
            if str(mirror).strip().rstrip("/") not in disabled_set
        ]
    input_target = target if target is not None else kind_defaults.get("target")
    input_sample_mb = sample_mb if sample_mb is not None else kind_defaults.get("sample_mb")

    normalized_mirrors = dedupe_mirrors(input_mirrors or [])
    normalized_target = str(input_target or default_target_for_kind(normalized_kind)).strip()
    normalized_sample_mb = int(DEFAULT_SAMPLE_MB if input_sample_mb is None else input_sample_mb)

    if not normalized_mirrors:
        raise ValueError("mirrors is required")
    if not normalized_target:
        raise ValueError("target is required")
    if normalized_sample_mb <= 0:
        raise ValueError("sample_mb must be positive")

    return {
        "kind": normalized_kind,
        "mirrors": normalized_mirrors,
        "target": normalized_target,
        "sample_mb": normalized_sample_mb,
        "probe_only": bool(probe_only),
    }


def summarize_mirror_list(mirrors, limit=3):
    items = [str(item).strip() for item in (mirrors or []) if str(item).strip()]
    if not items:
        return ""
    if len(items) == 1:
        return f"mirror={items[0]}"
    visible = ", ".join(items[:limit])
    if len(items) > limit:
        visible += f", ...(+{len(items) - limit})"
    return f"mirrors=[{visible}]"


def build_docker_pull_display(mirror, target):
    mirror_text = re.sub(r"^https?://", "", str(mirror or "").strip()).rstrip("/")
    target_text = str(target or "").strip()
    if not mirror_text or not target_text:
        return ""
    return f"docker pull {mirror_text}/{target_text}"


def summarize_request_subject(kind, mirrors, target):
    items = [str(item).strip() for item in (mirrors or []) if str(item).strip()]
    if kind == "docker" and len(items) == 1:
        display = build_docker_pull_display(items[0], target)
        if display:
            return f'subject="{display}"'
    return summarize_mirror_list(items)


def resolve_result_log_subject(result):
    if not isinstance(result, dict):
        return ""
    kind = str(result.get("kind") or "").strip().lower()
    if kind == "docker":
        display = build_docker_pull_display(result.get("mirror"), result.get("target"))
        if display:
            return display
    subject_detail = str((result.get("subject") or {}).get("detail") or "").strip()
    if subject_detail:
        return subject_detail
    download = result.get("download") or {}
    download_url = str(download.get("url") or "").strip()
    if download_url:
        return download_url
    redirect_url = str(download.get("location") or "").strip()
    if redirect_url:
        return redirect_url
    return str(result.get("mirror") or "").strip()


def run_test_batch(kind, mirrors, target, sample_mb, progress_callback=None, probe_only=False):
    started = time.perf_counter()
    sample_bytes = sample_mb * 1024 * 1024
    workers = max(1, len(mirrors)) if DEFAULT_WORKERS <= 0 else min(DEFAULT_WORKERS, max(1, len(mirrors)))
    tester = TESTERS[kind]
    results = []
    total = len(mirrors)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(tester, mirror, target, sample_bytes, probe_only=probe_only): mirror
            for mirror in mirrors
        }
        try:
            for done, future in enumerate(as_completed(future_map), start=1):
                result = future.result()
                results.append(result)
                if progress_callback is not None:
                    progress_callback(done, total, result)
        except KeyboardInterrupt:
            for future in future_map:
                future.cancel()
            raise

    results.sort(
        key=lambda item: (
            0 if item.get("ok") else 1,
            -(item.get("download", {}).get("speed_bps") or 0),
            item["mirror"],
        )
    )
    return {
        "kind": kind,
        "target": target,
        "sample_mb": sample_mb,
        "elapsed_s": round(time.perf_counter() - started, 3),
        "results": results,
    }


INDEX_HTML_PATH = os.path.join(_resource_root_dir(), "index.html")


def _load_index_html():
    with open(INDEX_HTML_PATH, "r", encoding="utf-8") as f:
        return f.read()


class AppHandler(BaseHTTPRequestHandler):
    server_version = f"MirrorSpeedTest/{APP_VERSION}"

    def _send_json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html, status=200):
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        client = self.client_address[0] if self.client_address else "-"
        if self.path in ("/", "/index.html"):
            log_info(f"{client} GET {self.path}")
            self._send_html(_load_index_html())
            return
        if self.path == "/api/defaults":
            log_info(f"{client} GET /api/defaults")
            self._send_json(get_runtime_config_snapshot())
            return
        if self.path == "/api/update":
            try:
                payload = build_update_status()
            except Exception as error:
                log_error(f"{client} GET /api/update failed: {error}")
                self._send_json({"error": str(error)}, status=502)
                return
            self._send_json(payload)
            return
        log_warn(f"{client} GET {self.path} -> 404")
        self._send_json({"error": "not found"}, status=404)

    def do_POST(self):
        client = self.client_address[0] if self.client_address else "-"
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(content_length).decode("utf-8") or "{}")
        except Exception as error:
            log_warn(f"{client} POST {self.path} invalid JSON: {error}")
            self._send_json({"error": f"invalid JSON: {error}"}, status=400)
            return

        if self.path == "/api/test":
            try:
                request = normalize_test_request(
                    kind=payload.get("kind"),
                    mirrors=payload.get("mirrors"),
                    target=payload.get("target"),
                    sample_mb=payload.get("sample_mb"),
                    probe_only=payload.get("probe_only"),
                )
                mirror_text = summarize_request_subject(
                    request["kind"],
                    request["mirrors"],
                    request["target"],
                )
                log_info(
                    f"{client} POST /api/test start kind={request['kind']} {mirror_text} "
                    f"sample_mb={request['sample_mb']} target={request['target']} probe_only={request['probe_only']}"
                )
                response_payload = run_test_batch(
                    kind=request["kind"],
                    mirrors=request["mirrors"],
                    target=request["target"],
                    sample_mb=request["sample_mb"],
                    probe_only=request["probe_only"],
                )
            except Exception as error:
                log_error(f"{client} POST /api/test failed: {error}")
                self._send_json({"error": str(error)}, status=400)
                return
            ok_count = sum(1 for item in response_payload.get("results", []) if item.get("ok"))
            total_count = len(response_payload.get("results", []))
            log_result = (response_payload.get("results") or [{}])[0]
            result_subject = resolve_result_log_subject(log_result)
            result_text = f' subject="{result_subject}"' if result_subject else ""
            log_info(
                f"{client} POST /api/test done kind={request['kind']} ok={ok_count}/{total_count} "
                f"elapsed={response_payload.get('elapsed_s', '?')}s{result_text}"
            )
            self._send_json(response_payload)
            return

        if self.path == "/api/update":
            try:
                response_payload = prepare_update_download(
                    preferred_mirror=payload.get("preferred_mirror"),
                    preferred_speed_mbps=payload.get("preferred_speed_mbps"),
                )
            except Exception as error:
                log_error(f"{client} POST /api/update failed: {error}")
                self._send_json({"error": str(error)}, status=400)
                return
            log_info(f"{client} POST /api/update done: {response_payload.get('message', '')}")
            self._send_json(response_payload)
            return

        log_warn(f"{client} POST {self.path} -> 404")
        self._send_json({"error": "not found"}, status=404)


def run_server():
    host = str(os.environ.get("MST_HOST", "127.0.0.1")).strip() or "127.0.0.1"
    try:
        port = int(str(os.environ.get("MST_PORT", "58080")).strip() or "58080")
    except Exception:
        port = 58080

    while True:
        try:
            server = ThreadingHTTPServer((host, port), AppHandler)
            break
        except OSError as error:
            if error.errno != 98:
                raise
            log_warn(f"port {port} is in use, trying {port + 1}")
            port += 1
            if port > 65535:
                raise RuntimeError("no available port") from error

    log_info(f"listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log_info("shutting down...")
    finally:
        server.server_close()


def main(argv=None):
    _ = argv
    run_server()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
