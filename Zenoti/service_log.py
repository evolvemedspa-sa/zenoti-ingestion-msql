"""Write a service's own output to a file and upload it to Google Drive.

run_all.py has always done this for the pipeline's log. zconnect and the
Aesthetix sync are separate Railway services with their own images, so they
cannot reuse its log_path -- but their logs should land in the same Drive
folder, under their own prefixes, so one folder holds every service's run.

Teeing sys.stdout is the whole mechanism: both scripts print everything they
report (sync_hc.py even shadows print to stamp each line), so one wrapper
catches every line without touching a call site.

    path = service_log.start("hc_ingestion_logs")
    try:
        ...the run...
    finally:
        service_log.upload(path)      # also on failure, where it matters most

The upload is best-effort on purpose: a run that has already written its rows
must not fail because Drive did, and the same is true upside down -- the log of
a failed run is the one worth having, so it is uploaded from a finally block
rather than after a success check.
"""
import os
import sys
import tempfile
from datetime import datetime


class _Tee:
    """sys.stdout that writes to the real stream and to a file.

    The stream is written first: it is the one with a console behind it, and if
    that is where the run is being watched, it should still be the first to fail.
    """

    def __init__(self, stream, handle):
        self._stream = stream
        self._handle = handle

    def write(self, data):
        written = self._stream.write(data)
        self._handle.write(data)
        return written

    def flush(self):
        self._stream.flush()
        self._handle.flush()

    def __getattr__(self, name):
        # isatty, encoding, reconfigure, fileno: whatever print() or the script
        # asks for is the real stream's answer.
        return getattr(self._stream, name)


def start(prefix):
    """Begin logging. Returns the path the run's output is being written to.

    `prefix` is the Drive-side name stem and the timestamp is appended, so two
    runs never collide. LOG_DIR overrides the folder; the default is the system
    temp directory, because this file exists to be uploaded, not to be kept.
    """
    folder = os.getenv("LOG_DIR") or tempfile.gettempdir()
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{prefix}_{datetime.now():%Y%m%d_%H%M%S}.txt")
    sys.stdout = _Tee(sys.stdout, open(path, "w", encoding="utf-8"))
    return path


def configured():
    """True when Drive is set up at all -- the same two checks run_all.py makes."""
    return bool(os.getenv("GDRIVE_PARENT_FOLDER")
                and (os.getenv("GDRIVE_CREDENTIALS_JSON")
                     or os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json")))


def upload(path):
    """Upload the log to GDRIVE_PARENT_FOLDER. Never raises.

    Flushed before and after: the tee's file handle is still open and buffered,
    so without the second flush the lines this function itself prints -- the
    upload result, which is the one a reader looks for -- never reach the file.

    The folder name is not printed and neither is its id: the log lands on Drive
    and is read by people, so a line naming the folder would carry the id with
    it. gdrive_helper prints the file's own name, which is the part worth seeing.
    """
    sys.stdout.flush()
    result = None
    if not configured():
        print("GDRIVE_PARENT_FOLDER not set, skipping log upload.")
    else:
        try:
            from gdrive_helper import upload_file_to_gdrive
            upload_file_to_gdrive(
                path,
                os.getenv("GDRIVE_PARENT_FOLDER"),
                credentials_json=os.getenv("GDRIVE_CREDENTIALS_JSON"),
                credentials_file=os.getenv("GDRIVE_CREDENTIALS_FILE", "service_account.json"),
            )
            print(f"Log uploaded: {os.path.basename(path)}")
            result = path
        except Exception as exc:                  # noqa: BLE001 -- best-effort by design
            print(f"Log upload failed: {exc}")
    sys.stdout.flush()
    return result


def _demo():
    """The tee is the part that could silently stop logging -- pin it."""
    import io

    real, handle = io.StringIO(), io.StringIO()
    tee = _Tee(real, handle)
    print("one line", file=tee, flush=True)
    tee.write("no newline")
    assert real.getvalue() == "one line\nno newline", repr(real.getvalue())
    assert handle.getvalue() == "one line\nno newline", repr(handle.getvalue())

    # No Drive configuration is not an error, and must not raise. Cleared first so
    # the stdout test below cannot upload the self-check's own file when a
    # developer happens to have Drive set up.
    saved = {name: os.environ.get(name) for name in
             ("GDRIVE_PARENT_FOLDER", "GDRIVE_CREDENTIALS_JSON", "GDRIVE_CREDENTIALS_FILE")}
    for name in saved:
        os.environ.pop(name, None)
    assert configured() is False
    assert upload("/nonexistent/nothing.txt") is None

    # start() redirects the real stdout; put it back before anything else prints.
    original, path = sys.stdout, None
    try:
        path = start("service_log_selftest")
        print("captured")
        upload(path)          # its own lines have to land in the file too
    finally:
        sys.stdout = original
    try:
        with open(path, encoding="utf-8") as fh:
            logged = fh.read()
        # The line upload() prints is the one a reader looks for, and it went
        # missing when the tee's handle was left unflushed after the last print.
        assert logged.startswith("captured\n"), repr(logged)
        assert "log upload." in logged, repr(logged)
    finally:
        os.remove(path)

    # Configured: the helper is called with the folder and the credentials the
    # environment names, and nothing about the folder reaches stdout. The real
    # Drive credentials exist only in the deployment, so the helper is stubbed --
    # this pins the wiring, which is the part that would silently stop uploading.
    import types

    calls = []
    fake = types.ModuleType("gdrive_helper")
    setattr(fake, "upload_file_to_gdrive",
            lambda path, folder, **kw: calls.append((path, folder, kw)))
    original_module = sys.modules.get("gdrive_helper")
    try:
        sys.modules["gdrive_helper"] = fake
        os.environ.update({"GDRIVE_PARENT_FOLDER": "FOLDER-ID-PLACEHOLDER",
                           "GDRIVE_CREDENTIALS_JSON": "{}"})
        os.environ.pop("GDRIVE_CREDENTIALS_FILE", None)
        assert configured() is True
        assert upload("C:/tmp/run.txt") == "C:/tmp/run.txt"
        assert calls == [("C:/tmp/run.txt", "FOLDER-ID-PLACEHOLDER",
                          {"credentials_json": "{}", "credentials_file": "service_account.json"})], calls
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        if original_module is None:
            sys.modules.pop("gdrive_helper", None)
        else:
            sys.modules["gdrive_helper"] = original_module

    print("service_log self-check OK")


if __name__ == "__main__":
    _demo()
