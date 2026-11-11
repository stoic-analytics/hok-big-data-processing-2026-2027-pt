"""Download Apache Kafka (binary, KRaft mode) and prepare a single-node broker for Session 6.

Only uses the Python standard library, like get_data.py. Works on macOS and Linux (and in the Codespace).
Windows: use the Docker route (README.md, route A) or run this script inside WSL.
The folder must NOT contain a space (the Kafka shell scripts then fail with "Classpath is empty").
If your repository folder has a space, use e.g.   python get_kafka.py --dest ~/kafka-course

Needs Java 17 or newer:   macOS: brew install openjdk@17   ·   Ubuntu: sudo apt install openjdk-17-jre-headless

Usage:  python get_kafka.py              # download, verify, extract, format storage, print the start command
        python get_kafka.py --start      # ... and start the broker right away (Ctrl+C stops it)
        python get_kafka.py --reformat   # wipe the data folder and format again (after a version change)
        python get_kafka.py --dest ~/kafka-course   # install somewhere else (path without spaces)

What ends up where (both folders are ignored by git):
    session-06/kafka_2.13-4.4.0/     the Kafka distribution (bin/, config/, libs/)
    session-06/kafka-data/           the broker's log segments  → this is where your messages live on disk
"""
import argparse, hashlib, os, pathlib, platform, re, shutil, subprocess, sys, tarfile, urllib.request

KAFKA_VERSION = "4.4.0"
SCALA_VERSION = "2.13"
MIRRORS = [  # first one that answers wins
    "https://dlcdn.apache.org/kafka/{v}/{f}",
    "https://archive.apache.org/dist/kafka/{v}/{f}",
]
CHECKSUM = "https://downloads.apache.org/kafka/{v}/{f}.sha512"
CHECKSUM_FALLBACK = "https://archive.apache.org/dist/kafka/{v}/{f}.sha512"
HERE = pathlib.Path(__file__).resolve().parent


def java_major():
    """Return the major version of `java` on PATH (or JAVA_HOME), or None."""
    java = shutil.which("java")
    if os.environ.get("JAVA_HOME"):
        cand = pathlib.Path(os.environ["JAVA_HOME"]) / "bin" / "java"
        if cand.exists():
            java = str(cand)
    if not java:
        return None, None
    try:
        out = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None, java
    m = re.search(r'version "(\d+)(?:\.(\d+))?', out.stderr + out.stdout)
    if not m:  # e.g. the macOS stub "Unable to locate a Java Runtime"
        return None, java
    major = int(m.group(1))
    return (int(m.group(2)) if major == 1 else major), java   # "1.8.0" means Java 8


def download(url, f):
    print(f"↓ {url}")
    tmp = f.with_suffix(f.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": "hok-big-data-course"})
    with urllib.request.urlopen(req, timeout=120) as r, open(tmp, "wb") as fh:
        shutil.copyfileobj(r, fh, length=1 << 20)
    tmp.replace(f)                       # only complete files get the final name
    print(f"  {f.stat().st_size/1e6:.1f} MB")


def fetch_text(url):
    req = urllib.request.Request(url, headers={"User-Agent": "hok-big-data-course"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", "replace")


def expected_sha512(fname, version):
    """Apache publishes 'kafka_x.tgz: F5CE BA8A ...' (gpg --print-md style) – strip name and spaces."""
    for tpl in (CHECKSUM, CHECKSUM_FALLBACK):
        try:
            txt = fetch_text(tpl.format(v=version, f=fname))
        except Exception as e:  # noqa: BLE001 – try the next source
            print(f"  (checksum not available from {tpl.split('/')[2]}: {e})")
            continue
        if fname in txt:                 # drop the file name – its digits are not part of the hash
            txt = txt.split(fname, 1)[1]
        hexdigits = re.sub(r"[^0-9a-fA-F]", "", txt).lower()
        if len(hexdigits) == 128:
            return hexdigits
    return None


def sha512_of(path):
    h = hashlib.sha512()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_course_config(kafka_dir, data_dir):
    """Copy config/server.properties → config/course-server.properties with our data folder,
    automatic topic creation switched off (you create topics yourself) and small segments
    (so you can see several segment files in kafka-data/ after a few thousand messages)."""
    src = (kafka_dir / "config" / "server.properties").read_text()
    overrides = {
        "log.dirs": data_dir.as_posix(),
        "auto.create.topics.enable": "false",
        "num.partitions": "1",
        "log.segment.bytes": str(1024 * 1024),     # 1 MB segments (default 1 GB)
    }
    lines = [ln for ln in src.splitlines() if ln.split("=", 1)[0].strip() not in overrides]
    lines += ["", "# --- Session 6 course overrides (written by get_kafka.py) ---"]
    lines += [f"{k}={v}" for k, v in overrides.items()]
    cfg = kafka_dir / "config" / "course-server.properties"
    cfg.write_text("\n".join(lines) + "\n")
    return cfg


def format_storage(kafka_dir, cfg, data_dir, version, reformat):
    marker = data_dir / ".formatted-with-kafka"
    if reformat and data_dir.exists():
        print(f"✗ deleting {data_dir} (all topics and messages are gone)")
        shutil.rmtree(data_dir)
    if (data_dir / "meta.properties").exists():
        old = marker.read_text().strip() if marker.exists() else "unknown"
        if old != version:
            print(f"⚠ {data_dir.name}/ was formatted with Kafka {old}, you now use {version}.\n"
                  f"  If the broker refuses to start:  python get_kafka.py --reformat  (deletes all messages)")
        else:
            print(f"✓ storage in {data_dir.name}/ already formatted")
        return
    storage = kafka_dir / "bin" / "kafka-storage.sh"
    cluster_id = subprocess.run([str(storage), "random-uuid"], capture_output=True, text=True,
                                check=True).stdout.strip().splitlines()[-1]
    print(f"• formatting storage (cluster id {cluster_id}) …")
    subprocess.run([str(storage), "format", "--standalone", "-t", cluster_id, "-c", str(cfg)], check=True)
    data_dir.mkdir(exist_ok=True)
    marker.write_text(version + "\n")


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--version", default=KAFKA_VERSION, help=f"Kafka version (default {KAFKA_VERSION})")
    p.add_argument("--scala", default=SCALA_VERSION, help=f"Scala build (default {SCALA_VERSION})")
    p.add_argument("--dest", type=pathlib.Path, default=HERE, help="where to extract (default: session-06/)")
    p.add_argument("--start", action="store_true", help="start the broker in this terminal afterwards")
    p.add_argument("--reformat", action="store_true", help="delete kafka-data/ and format it again")
    p.add_argument("--no-verify", action="store_true", help="skip the SHA-512 check")
    a = p.parse_args()

    if platform.system() == "Windows":
        sys.exit("Windows: the Kafka shell scripts need a Unix shell. Use Docker (README.md, route A) or WSL.")

    dest = a.dest.expanduser().resolve()
    if " " in str(dest):
        sys.exit(f"✗ The install folder contains a space:\n    {dest}\n"
                 "  Kafka's shell scripts cannot handle that (error: 'Classpath is empty').\n"
                 "  Install to a folder without spaces, for example:\n"
                 "    python get_kafka.py --dest ~/kafka-course\n"
                 "  …or use Docker (README.md, route A).")
    major, java = java_major()
    if major is None:
        sys.exit("✗ No working Java found. Kafka 4.x needs Java 17+.\n"
                 "  macOS: brew install openjdk@17   (then follow the 'sudo ln -sfn …' hint brew prints)\n"
                 "  Ubuntu/Debian: sudo apt install openjdk-17-jre-headless\n"
                 "  …or use Docker (README.md, route A) – no Java needed.")
    if major < 17:
        sys.exit(f"✗ {java} is Java {major}. Kafka 4.x needs Java 17+. Install a newer JDK or set JAVA_HOME.")
    print(f"✓ Java {major} ({java})")

    name = f"kafka_{a.scala}-{a.version}"
    fname = f"{name}.tgz"
    dest.mkdir(parents=True, exist_ok=True)
    kafka_dir, data_dir, tgz = dest / name, dest / "kafka-data", dest / fname

    if (kafka_dir / "bin" / "kafka-server-start.sh").exists():
        print(f"✓ {name}/ already there")
    else:
        if not tgz.exists():
            for tpl in MIRRORS:
                try:
                    download(tpl.format(v=a.version, f=fname), tgz)
                    break
                except Exception as e:  # noqa: BLE001 – try the next mirror
                    print(f"  failed: {e}")
            else:
                sys.exit("✗ Download failed from all mirrors. Check the Wi-Fi or use Docker (route A).")
        if not a.no_verify:
            want = expected_sha512(fname, a.version)
            if want is None:
                print("⚠ could not fetch the official checksum – continuing without verification")
            elif sha512_of(tgz) != want:
                tgz.unlink()
                sys.exit("✗ SHA-512 mismatch – the download is corrupt and was deleted. Run the script again.")
            else:
                print("✓ SHA-512 checksum matches the Apache release")
        print(f"• extracting to {kafka_dir} …")
        with tarfile.open(tgz) as tf:
            if hasattr(tarfile, "data_filter"):
                tf.extractall(dest, filter="data")
            else:
                tf.extractall(dest)  # Python < 3.12 without the filter argument
        tgz.unlink()                 # 130 MB we no longer need

    cfg = write_course_config(kafka_dir, data_dir)
    format_storage(kafka_dir, cfg, data_dir, a.version, a.reformat)

    start = [str(kafka_dir / "bin" / "kafka-server-start.sh"), str(cfg)]
    rel = lambda x: os.path.relpath(x)                        # noqa: E731 – shorter, copy-pasteable paths
    print("\nStart the broker (leave this terminal open, Ctrl+C stops it):")
    print(f'    "{rel(start[0])}" "{rel(start[1])}"')
    print("CLI tools for the lab:")
    print(f'    export PATH="{kafka_dir / "bin"}:$PATH"     # then e.g. kafka-topics.sh --bootstrap-server localhost:9092 --list')
    if a.start:
        print("\n• starting Kafka … (Ctrl+C to stop)")
        env = dict(os.environ)
        env.setdefault("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")   # laptop-friendly; the default is 1 GB
        try:
            subprocess.run(start, env=env)
        except KeyboardInterrupt:
            print("\nbroker stopped")


if __name__ == "__main__":
    main()
