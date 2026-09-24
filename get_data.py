"""Download NYC Yellow Taxi trip data (Parquet) into ./data.

Usage:  python get_data.py            # 2024-01 .. 2024-06 (~300 MB)
        python get_data.py --months 12
"""
import argparse, pathlib, requests

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{y}-{m:02d}.parquet"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, default=2024)
    p.add_argument("--months", type=int, default=6)
    a = p.parse_args()
    out = pathlib.Path(__file__).parent / "data"
    out.mkdir(exist_ok=True)
    for m in range(1, a.months + 1):
        f = out / f"yellow_tripdata_{a.year}-{m:02d}.parquet"
        if f.exists():
            print(f"✓ {f.name} already there"); continue
        url = BASE.format(y=a.year, m=m)
        print(f"↓ {url}")
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(f, "wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
        print(f"  {f.stat().st_size/1e6:.0f} MB")

if __name__ == "__main__":
    main()
