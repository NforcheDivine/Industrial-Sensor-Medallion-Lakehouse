import pandas as pd
import numpy as np
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# -------------------------------------------------------
# Always write to PROJECT ROOT /data/landing/telemetry
# (Fixes the issue where files went into src\data\...)
# -------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]   # one level above /src
LANDING_DIR = PROJECT_ROOT / "data" / "landing" / "telemetry"
LANDING_DIR.mkdir(parents=True, exist_ok=True)

# Simulated fleet of devices
DEVICE_IDS = [f"device_{i:03d}" for i in range(1, 51)]  # 50 devices

# Micro-batch settings (start small and stable)
BATCH_SIZE = 2000
SLEEP_SECONDS = 5

# "Real-world messiness" rates
BAD_RATE = 0.02        # 2% bad rows (impossible physics)
DUPLICATE_RATE = 0.01  # 1% exact duplicates (idempotency test)
LATE_RATE = 0.02       # 2% late events
LATE_MINUTES = 30      # late by up to 30 minutes


def make_batch(batch_size: int) -> pd.DataFrame:
    now = datetime.now(timezone.utc)

    df = pd.DataFrame({
        "event_id": [str(uuid.uuid4()) for _ in range(batch_size)],
        "event_time": [now] * batch_size,
        "device_id": np.random.choice(DEVICE_IDS, size=batch_size),
        "temperature_c": np.random.normal(loc=45, scale=6, size=batch_size),
        "pressure_kpa": np.random.normal(loc=100, scale=12, size=batch_size),
        "humidity_pct": np.random.uniform(20, 80, size=batch_size),
        "vibration_rms": np.random.normal(loc=2.5, scale=0.5, size=batch_size),
        "energy_kw": np.random.normal(loc=300, scale=40, size=batch_size),
    })

    # Late events: event_time occurs earlier than "now"
    late_n = int(batch_size * LATE_RATE)
    if late_n > 0:
        late_idx = np.random.choice(df.index, size=late_n, replace=False)
        late_offsets = np.random.randint(1, LATE_MINUTES + 1, size=late_n)
        df.loc[late_idx, "event_time"] = (
            pd.to_datetime(df.loc[late_idx, "event_time"])
            - pd.to_timedelta(late_offsets, unit="m")
        )

    # Bad data: physical impossibilities
    bad_n = int(batch_size * BAD_RATE)
    if bad_n > 0:
        bad_idx = np.random.choice(df.index, size=bad_n, replace=False)
        df.loc[bad_idx, "pressure_kpa"] = -1  # impossible
        half = bad_n // 2
        if half > 0:
            df.loc[bad_idx[:half], "humidity_pct"] = 150  # impossible (>100)

    # Duplicates: copy some rows with same event_id (tests dedupe)
    dup_n = int(batch_size * DUPLICATE_RATE)
    if dup_n > 0:
        dup_rows = df.sample(dup_n, random_state=42).copy()
        df = pd.concat([df, dup_rows], ignore_index=True)

    return df


def write_batch(df: pd.DataFrame, batch_id: int) -> Path:
    # Use timezone-aware UTC time (no deprecation warning)
    fname = f"telemetry_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{batch_id:06d}.csv"
    path = LANDING_DIR / fname
    df.to_csv(path, index=False)
    return path


if __name__ == "__main__":
    print("✅ Starting telemetry micro-batches. Press Ctrl+C to stop.")
    batch_id = 0
    try:
        while True:
            df = make_batch(BATCH_SIZE)
            out = write_batch(df, batch_id)
            print(f"🟢 Wrote {len(df):,} rows → {out}")
            batch_id += 1
            time.sleep(SLEEP_SECONDS)
    except KeyboardInterrupt:
        print("\n🛑 Stopped generator.")