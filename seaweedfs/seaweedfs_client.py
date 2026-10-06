# seaweedfs_client.py
# Lightweight client for the SeaweedFS Filer HTTP API (implemented with the standard library urllib, no third-party dependencies).
# This is the shared low-level wrapper for the 5 seaweedfs_*.py scripts, equivalent in role to
# hdfs.InsecureClient in the HDFS scripts — HDFS has an existing library; SeaweedFS does not, so urllib is used here.
#
# Interface behavior has been verified against SeaweedFS 4.46 (http://192.168.31.111:8888):
#   - GET  /healthz                 health check -> 200
#   - POST /dir/                    create directory -> 201; already exists -> 409
#   - POST /dir/?maxMB=N (multipart) upload file -> 201 {"name","size"}; overwrite same name -> 201
#   - GET  /dir/?limit=N&lastFileName=X (Accept: application/json) list directory -> 200 JSON
#   - GET  /file?metadata=true      file metadata -> 200 JSON (chunks key in lowercase)
#   - GET  /file                    download file content -> 200 body
#   - POST /dest?mv.from=/src       move/rename (used for soft delete) -> 204
#   - DELETE /file or /dir/?recursive=true  delete -> 204
import os
import sys
import json
import time
import re
import urllib.request
import urllib.parse
import urllib.error
import uuid
from datetime import datetime

# Reuse the configuration from the parent directory smallfiles_heuristic
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from config import Config

# SeaweedFS directory bit flag (os.ModeDir): check Mode field via bitwise AND to determine if it is a directory
_MODE_DIR_BIT = 0x80000000


def _parse_mtime_to_ms(mtime_str):
    """
    Convert the RFC3339 time string returned by SeaweedFS to a millisecond timestamp (aligned with HDFS modificationTime).
    SeaweedFS Mtime looks like '2026-09-15T13:22:13.473810343+08:00' (nanosecond precision),
    Python's fromisoformat only accepts up to microseconds, so fractional seconds are truncated to 6 digits first.
    :return: millisecond timestamp (int); returns 0 on parse failure
    """
    if not mtime_str:
        return 0
    try:
        s = re.sub(r"(\.\d{6})\d+", r"\1", mtime_str)  # Truncate precision above nanoseconds/microseconds to microseconds
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


class SeaweedFSFilerClient:
    """SeaweedFS Filer HTTP client, wrapping atomic operations such as upload/download/list/metadata/move/delete."""

    def __init__(self, base_url=None, chunk_size_mb=None, dir_list_limit=None):
        self.base_url = (base_url or Config.SEAWEDFS_FILER_URL).rstrip("/")
        # Force chunk size via maxMB during upload so that 1 small file exclusively occupies 1 chunk (aligned with HDFS 1 block)
        self.chunk_size_mb = chunk_size_mb or Config.SEAWEDFS_CHUNK_SIZE_MB
        self.chunk_size_bytes = self.chunk_size_mb * 1024 * 1024
        self.dir_list_limit = dir_list_limit or Config.SEAWEDFS_DIR_LIST_LIMIT

    # ================= Low-Level HTTP =================

    def _build_url(self, path, params=None):
        url = self.base_url + urllib.parse.quote(path, safe="/")
        if params:
            url += "?" + urllib.parse.urlencode(params)
        return url

    def _request(self, method, path, data=None, headers=None, params=None, timeout=120):
        """Issue one HTTP request and return (status_code, headers_dict, body_bytes)."""
        req = urllib.request.Request(self._build_url(path, params), data=data, method=method)
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers or {}), e.read()

    def _request_json(self, method, path, params=None, headers=None):
        """Issue a request and parse the JSON body; return (status_code, dict_or_None)."""
        status, _, body = self._request(method, path, params=params, headers=headers)
        if not body:
            return status, None
        try:
            return status, json.loads(body)
        except Exception:
            return status, None

    # ================= Connection Check =================

    def health_check(self):
        """
        Check whether the Filer is available. Probe /healthz first; on failure, fall back to the root directory listing.
        Raise ValueError when unreachable (consistent with the connection-check style of HDFS scripts).
        """
        status, _, _ = self._request("GET", "/healthz", timeout=15)
        if status == 200:
            return True
        # Some deployments may not expose /healthz; fall back to probing the root directory JSON listing
        status, _, _ = self._request("GET", "/", params={"limit": 1},
                                     headers={"Accept": "application/json"}, timeout=15)
        if status == 200:
            return True
        raise ValueError(f"❌ 无法连接到 SeaweedFS Filer ({self.base_url})，请检查服务是否启动。HTTP {status}")

    # ================= Directory Operations =================

    def exists(self, path):
        """Check whether a file/directory exists (GET metadata; 404 means not found)."""
        clean = path.rstrip("/") or "/"
        if path.endswith("/"):  # Directory: list it; if listing succeeds, it exists
            status, _, _ = self._request("GET", clean + "/", params={"limit": 1},
                                         headers={"Accept": "application/json"})
        else:
            status, _, _ = self._request("GET", clean, params={"metadata": "true"})
        return status == 200

    def mkdir(self, path):
        """
        Create a single directory. Treat 409 (already exists) as success.
        :return: True if the directory is ready, False if creation failed
        """
        clean = path.rstrip("/")
        if not clean or clean == "/":
            return True
        status, _, body = self._request("POST", clean + "/")  # A trailing / with no Content-Type triggers mkdir
        if status in (201, 409):  # 409 = already exists
            return True
        detail = body.decode("utf-8", "ignore")[:200] if body else ""
        print(f"⚠️ 创建目录失败 {clean}: HTTP {status} {detail}")
        return False

    def makedirs(self, path):
        """Create directories level by level (similar to os.makedirs); existing levels are skipped automatically."""
        clean = path.rstrip("/")
        if not clean or clean == "/":
            return True
        parts = [p for p in clean.split("/") if p]
        cur = ""
        for part in parts:
            cur += "/" + part
            if not self.mkdir(cur):
                return False
        return True

    def list_dir(self, path):
        """
        List all entries under a directory (with automatic pagination); return a list of entry dicts.
        Each entry contains at least FullPath / Mode / FileSize / Mtime (see Filer JSON).
        Note: during Filer pagination, subdirectories are returned only on the first page, so walk recursion depends on subdirs from the first page.
        """
        clean = path.rstrip("/") or "/"
        dir_path = clean if clean.endswith("/") else clean + "/"
        entries = []
        last_file_name = ""
        while True:
            params = {"limit": self.dir_list_limit}
            if last_file_name:
                params["lastFileName"] = last_file_name
            status, data = self._request_json("GET", dir_path, params=params,
                                              headers={"Accept": "application/json"})
            if status == 404:
                return entries  # Directory does not exist; return empty
            if status != 200 or not data:
                print(f"⚠️ 列目录失败 {dir_path}: HTTP {status}")
                return entries
            batch = data.get("Entries") or []
            entries.extend(batch)
            if data.get("ShouldDisplayLoadMore") and data.get("LastFileName"):
                last_file_name = data["LastFileName"]
            else:
                break
        return entries

    def walk(self, root_path):
        """
        Recursively traverse the directory tree, yielding (dirpath, subdirs, filenames) triples (consistent with hdfs client.walk / os.walk).
        filenames is a list of file names; subdirs is a list of subdirectory names.
        """
        stack = [root_path.rstrip("/") or "/"]
        while stack:
            cur = stack.pop()
            dir_path = cur if cur.endswith("/") else cur + "/"
            entries = self.list_dir(dir_path)
            subdirs, filenames = [], []
            for e in entries:
                full = e.get("FullPath", "")
                name = full.rsplit("/", 1)[-1]
                if not name:
                    continue
                if e.get("Mode", 0) & _MODE_DIR_BIT:
                    subdirs.append(name)
                    stack.append((cur.rstrip("/") + "/" + name))
                else:
                    filenames.append(name)
            yield cur, subdirs, filenames

    def walk_entries(self, root_path):
        """
        Recursively traverse the directory tree, yielding (dirpath, file_entries) pairs.
        file_entries is the raw list of entry dicts returned by directory listing (including FullPath / FileSize / Mode, etc.),
        Callers can derive block statistics directly from FileSize without issuing an extra metadata request per file.
        Optimized for \"traverse + aggregate\" scenarios such as seaweedfs_remove.py.
        """
        stack = [root_path.rstrip("/") or "/"]
        while stack:
            cur = stack.pop()
            dir_path = cur if cur.endswith("/") else cur + "/"
            entries = self.list_dir(dir_path)
            file_entries = []
            for e in entries:
                full = e.get("FullPath", "")
                name = full.rsplit("/", 1)[-1]
                if not name:
                    continue
                if e.get("Mode", 0) & _MODE_DIR_BIT:
                    stack.append(cur.rstrip("/") + "/" + name)
                else:
                    file_entries.append(e)
            yield cur, file_entries

    # ================= File Metadata =================

    def stat(self, path):
        """
        Get file metadata (GET ?metadata=true).
        :return: dict containing normalized fields such as file_size / chunk_count / chunk_sizes / mtime_ms / mime;
                 returns None if the file does not exist or on error.
        """
        status, data = self._request_json("GET", path, params={"metadata": "true"})
        if status != 200 or not data:
            return None
        chunks = data.get("chunks") or []  # Note: chunks is a lowercase key (struct json tag)
        chunk_sizes = [c.get("size", 0) for c in chunks]
        file_size = data.get("FileSize", 0) or 0
        return {
            "full_path": data.get("FullPath", path),
            "file_size": file_size,
            "chunk_count": len(chunks),
            "chunk_sizes": chunk_sizes,
            "mtime_ms": _parse_mtime_to_ms(data.get("Mtime", "")),
            "mime": data.get("Mime", ""),
        }

    # ================= Upload =================

    def upload_file(self, local_path, filer_path, max_mb=None):
        """
        Upload a local file to the specified Filer path via multipart/form-data (same-name overwrite).
        Use maxMB to force chunk size so each small file exclusively occupies 1 chunk (aligned with HDFS baseline 1 block).
        The Filer automatically creates missing parent directories (verified empirically).
        :return: True on success / False on failure
        """
        max_mb = max_mb or self.chunk_size_mb
        parent = filer_path.rsplit("/", 1)[0] or "/"
        filename = filer_path.rsplit("/", 1)[-1]
        # POST to the parent directory (ending with /); the landed file name is determined by the multipart filename
        target = parent.rstrip("/") + "/"

        boundary = "----seaweedfs" + uuid.uuid4().hex
        with open(local_path, "rb") as fh:
            file_bytes = fh.read()
        head = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
            f"Content-Type: application/octet-stream\r\n\r\n"
        ).encode("utf-8")
        tail = f"\r\n--{boundary}--\r\n".encode("utf-8")
        body = head + file_bytes + tail

        status, _, resp = self._request(
            "POST", target, data=body, params={"maxMB": max_mb},
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            timeout=600,
        )
        if status in (200, 201):
            return True
        detail = resp.decode("utf-8", "ignore")[:200] if resp else ""
        print(f"   ❌ 上传失败 {filename}: HTTP {status} {detail}")
        return False

    def upload_bytes(self, data_bytes, filer_path, max_mb=None):
        """Upload in-memory bytes to a Filer path (same-name overwrite), for scenarios that do not require a temporary file on disk."""
        max_mb = max_mb or self.chunk_size_mb
        parent = filer_path.rsplit("/", 1)[0] or "/"
        filename = filer_path.rsplit("/", 1)[-1]
        target = parent.rstrip("/") + "/"
        boundary = "----seaweedfs" + uuid.uuid4().hex
        head = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
            f"Content-Type: application/octet-stream\r\n\r\n"
        ).encode("utf-8")
        body = head + data_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")
        status, _, resp = self._request(
            "POST", target, data=body, params={"maxMB": max_mb},
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            timeout=600,
        )
        return status in (200, 201)

    # ================= Download =================

    def download(self, filer_path):
        """
        Download file content.
        :return: (ok: bool, data: bytes). ok=False when the file does not exist or on error.
        """
        status, _, body = self._request("GET", filer_path, timeout=600)
        if status == 200:
            return True, body
        return False, b""

    # ================= Move / Delete =================

    def move(self, src_path, dst_path):
        """
        Move/rename: POST dst?mv.from=src (soft delete relies on this to move files into the trash directory).
        :return: True on success (204) / False on failure
        """
        status, _, body = self._request("POST", dst_path, params={"mv.from": src_path})
        if status in (200, 204):
            return True
        detail = body.decode("utf-8", "ignore")[:200] if body else ""
        print(f"   ❌ 移动失败 {src_path} -> {dst_path}: HTTP {status} {detail}")
        return False

    def delete(self, path, recursive=False):
        """Delete a file or directory. Directories require recursive=True. :return: True (204) / False."""
        params = {"recursive": "true"} if recursive else None
        clean = path if not path.endswith("/") else path.rstrip("/") + "/"
        status, _, body = self._request("DELETE", clean, params=params)
        if status in (200, 204):
            return True
        detail = body.decode("utf-8", "ignore")[:200] if body else ""
        print(f"   ❌ 删除失败 {path}: HTTP {status} {detail}")
        return False
