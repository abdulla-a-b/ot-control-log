"""
OT Control Management — Processor (v2)
Reads attendance Excel/CSV from /data/, aggregates weekly hours,
outputs docs/data/ot_data.json for the GitHub Pages dashboard.

Supports real attendance file format:
  Date | Employee ID | Emp. Name | Designation | Floor | Units | Shift |
  OT | In Time | Out Time | Department | Section | Team | Line | Gender
"""

import os, json, glob, re, math
from datetime import date, datetime, timedelta
from collections import defaultdict
import pandas as pd

def safe_num(v, default=0):
    """Return 0 (or default) for NaN/None/inf — keeps JSON valid."""
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return default
        return round(f, 2)
    except (TypeError, ValueError):
        return default

def sanitize(obj):
    """Recursively replace NaN/inf/None with JSON-safe values."""
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return 0
        return obj
    return obj

WEEKLY_LIMIT = 72
DATA_DIR     = "data"
OUTPUT_PATH  = "docs/data/ot_data.json"

RISK_CFG = [
    ("exceeded", 72,  9999, "#A855F7", "#4A044E"),
    ("critical", 68,  72,   "#EF4444", "#7F1D1D"),
    ("warning",  60,  68,   "#F97316", "#7C2D12"),
    ("caution",  48,  60,   "#EAB308", "#78350F"),
    ("safe",      0,  48,   "#22C55E", "#052E16"),
]

def classify(h):
    for name, lo, hi, *_ in RISK_CFG:
        if lo <= h < hi: return name
    return "exceeded"

def risk_order(name):
    m = {"exceeded":5,"critical":4,"warning":3,"caution":2,"safe":1}
    return m.get(name, 0)

def week_start(d):
    return d - timedelta(days=d.weekday())

def quarter_of(d):
    return f"{d.year}-Q{(d.month-1)//3+1}"

def parse_time_hours(t_str):
    """Parse HH:MM or HH:MM:SS string → decimal hours."""
    if pd.isna(t_str): return None
    s = str(t_str).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(s, fmt)
            return t.hour + t.minute/60 + t.second/3600
        except: pass
    return None

def calc_worked_hours(in_h, out_h):
    """Calculate total hours worked from decimal hour values."""
    if in_h is None or out_h is None: return None
    diff = out_h - in_h
    if diff < 0: diff += 24   # overnight shift
    return round(diff, 2)

# ── Column name mapping (handles variations) ─────────────────────────────
COL_ALIASES = {
    "date":        ["date"],
    "emp_id":      ["employee id","emp id","empid","employee_id","emp_no","id no"],
    "emp_name":    ["emp. name","emp name","employee name","name","full name","emp_name"],
    "designation": ["designation","title","job title","position"],
    "floor":       ["floor","building","location"],
    "unit":        ["units","unit","factory","plant"],
    "shift":       ["shift","shift code","shift_code"],
    "ot_hours":    ["ot","ot hours","overtime","overtime hours","ot_hours"],
    "in_time":     ["in time","in_time","time in","entry time","punch in"],
    "out_time":    ["out time","out_time","time out","exit time","punch out"],
    "department":  ["department","dept","division"],
    "section":     ["section","sub dept","sub-department"],
    "team":        ["team","group","work group"],
    "line":        ["line","production line","line no"],
    "gender":      ["gender","sex"],
}

def find_col(cols, candidates):
    for c in candidates:
        for dc in cols:
            if dc.strip().lower() == c: return dc
        # partial match fallback
        for dc in cols:
            if c in dc.strip().lower(): return dc
    return None

# ── Load all files ──────────────────────────────────────────────────────
def load_all():
    patterns = [
        os.path.join(DATA_DIR, "*.xlsx"),
        os.path.join(DATA_DIR, "*.xls"),
        os.path.join(DATA_DIR, "*.csv"),
    ]
    files = []
    for p in patterns:
        files.extend(sorted(glob.glob(p)))

    if not files:
        print("  No data files found in /data/")
        return pd.DataFrame()

    frames = []
    for fpath in files:
        try:
            ext = fpath.lower().split(".")[-1]
            if ext in ("xlsx", "xls"):
                # Try all sheets, use first non-empty one
                xl = pd.ExcelFile(fpath)
                for sheet in xl.sheet_names:
                    df = pd.read_excel(fpath, sheet_name=sheet, dtype=str)
                    if len(df) > 0:
                        df["_src"] = os.path.basename(fpath)
                        df["_sheet"] = sheet
                        frames.append(df)
                        print(f"  Loaded {len(df):>5} rows — {os.path.basename(fpath)} [{sheet}]")
                        break  # one sheet per file
            else:
                df = pd.read_csv(fpath, dtype=str, encoding="utf-8-sig")
                df["_src"] = os.path.basename(fpath)
                df["_sheet"] = "csv"
                frames.append(df)
                print(f"  Loaded {len(df):>5} rows — {os.path.basename(fpath)}")
        except Exception as e:
            print(f"  SKIP {os.path.basename(fpath)}: {e}")

    if not frames: return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)

# ── Normalise ────────────────────────────────────────────────────────────
def normalise(raw):
    col_map = {}
    raw_cols = raw.columns.tolist()
    for std_key, cands in COL_ALIASES.items():
        found = find_col(raw_cols, cands)
        if found: col_map[found] = std_key

    df = raw.rename(columns=col_map).copy()

    # Require at minimum: date, emp_id, ot_hours
    for req in ["date","emp_id","ot_hours"]:
        if req not in df.columns:
            print(f"  WARNING: Required column '{req}' not found. Skipping block.")
            return pd.DataFrame()

    # Parse dates
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=False).dt.date
    df = df[df["date"].notna()]

    # Parse OT
    df["ot_hours"] = pd.to_numeric(df["ot_hours"], errors="coerce").fillna(0).clip(lower=0)

    # Calculate worked hours from in/out times
    if "in_time" in df.columns and "out_time" in df.columns:
        df["_in_h"]  = df["in_time"].apply(parse_time_hours)
        df["_out_h"] = df["out_time"].apply(parse_time_hours)
        df["worked_hours"] = df.apply(
            lambda r: calc_worked_hours(r["_in_h"], r["_out_h"]), axis=1
        )
        # ── NaN guard: rows with unparseable Out Time (e.g. "--:--") ──
        # Fall back to 9.17h regular + OT for those rows
        nan_mask = df["worked_hours"].isna()
        if nan_mask.any():
            print(f"  NOTE: {nan_mask.sum()} rows have invalid In/Out times — using 9.17h+OT fallback")
        df.loc[nan_mask, "worked_hours"] = 9.17 + df.loc[nan_mask, "ot_hours"]
        df["worked_hours"] = pd.to_numeric(df["worked_hours"], errors="coerce").fillna(9.17)
        # Regular = worked - OT (floor at 0)
        df["regular_hours"] = (df["worked_hours"] - df["ot_hours"]).clip(lower=0)
        df["total_hours"]   = df["worked_hours"]
        df.drop(columns=["_in_h","_out_h"], inplace=True)
    else:
        # Fallback: assume 9.17h regular (typical shift)
        df["regular_hours"] = 9.17
        df["total_hours"]   = df["regular_hours"] + df["ot_hours"]
        df["worked_hours"]  = df["total_hours"]

    # Final safety net: replace any remaining NaN in numeric cols with 0
    for col in ["ot_hours","regular_hours","total_hours","worked_hours"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Fill optional string columns
    for col in ["emp_name","designation","floor","unit","shift",
                "department","section","team","line","gender"]:
        if col not in df.columns: df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    df["emp_id"] = df["emp_id"].fillna("").astype(str).str.strip()
    df = df[df["emp_id"] != ""]

    # Derived time fields
    df["week"]    = df["date"].apply(week_start)
    df["month"]   = df["date"].apply(lambda d: d.strftime("%Y-%m"))
    df["quarter"] = df["date"].apply(quarter_of)
    df["year"]    = df["date"].apply(lambda d: str(d.year))

    print(f"\n  Valid records: {len(df):,}")
    print(f"  Employees:     {df['emp_id'].nunique():,}")
    print(f"  Date range:    {df['date'].min()} → {df['date'].max()}")
    return df

# ── Employee info helper ─────────────────────────────────────────────────
def emp_info(grp):
    r = grp.iloc[-1]
    return {
        "emp_id":      str(r["emp_id"]),
        "emp_name":    str(r.get("emp_name","")),
        "designation": str(r.get("designation","")),
        "floor":       str(r.get("floor","")),
        "unit":        str(r.get("unit","")),
        "department":  str(r.get("department","")),
        "section":     str(r.get("section","")),
        "team":        str(r.get("team","")),
        "line":        str(r.get("line","")),
        "gender":      str(r.get("gender","")),
        "shift":       str(r.get("shift","")),
    }

# ── Group-by helper ──────────────────────────────────────────────────────
def group_by(emps, field):
    groups = defaultdict(lambda: {
        "total_hours":0,"ot_hours":0,"employees":0,
        "risk_counts":defaultdict(int)
    })
    for e in emps:
        key = e.get(field) or "—"
        g   = groups[key]
        g["total_hours"] += e.get("total_hours", 0)
        g["ot_hours"]    += e.get("ot_hours", 0)
        g["employees"]   += 1
        g["risk_counts"][e.get("risk_level","safe")] += 1
    result = {}
    for name, g in sorted(groups.items()):
        result[name] = {
            "avg_hours": round(g["total_hours"] / g["employees"], 1),
            "avg_ot":    round(g["ot_hours"]    / g["employees"], 1),
            "employees": g["employees"],
            "risk_counts": dict(g["risk_counts"]),
        }
    return result

def gender_split(emps):
    counts = defaultdict(lambda: {"employees":0,"avg_ot":0,"total_ot":0})
    for e in emps:
        g = e.get("gender","Unknown") or "Unknown"
        counts[g]["employees"]  += 1
        counts[g]["total_ot"]   += e.get("ot_hours",0)
    result = {}
    for k,v in counts.items():
        result[k] = {
            "employees": v["employees"],
            "avg_ot":    round(v["total_ot"]/v["employees"],1) if v["employees"] else 0,
        }
    return result

# ── DAILY ────────────────────────────────────────────────────────────────
def build_daily(df):
    daily = {}
    for d, dg in df.groupby("date"):
        emps = []
        for eid, eg in dg.groupby("emp_id"):
            info  = emp_info(eg)
            ot_h  = round(float(eg["ot_hours"].sum()), 1)
            tot_h = round(float(eg["total_hours"].sum()), 1)
            emps.append({**info, "ot_hours": ot_h, "total_hours": tot_h})
        emps.sort(key=lambda e: -e["ot_hours"])
        total_ot = round(sum(e["ot_hours"] for e in emps), 1)
        daily[str(d)] = {
            "date":       str(d),
            "day_name":   d.strftime("%A"),
            "employees":  len(emps),
            "total_ot":   total_ot,
            "avg_ot":     round(total_ot/len(emps),1) if emps else 0,
            "top10":      emps[:10],
            "all":        emps,
            "by_unit":    group_by(emps,"unit"),
            "by_floor":   group_by(emps,"floor"),
            "by_dept":    group_by(emps,"department"),
            "gender":     gender_split(emps),
        }
    return daily

# ── WEEKLY ───────────────────────────────────────────────────────────────
def build_weekly(df):
    weekly = {}
    for ws, wg in df.groupby("week"):
        emp_map = {}
        for eid, eg in wg.groupby("emp_id"):
            info      = emp_info(eg)
            days_w    = int(eg["date"].nunique())
            ot_h      = round(float(eg["ot_hours"].sum()), 1)
            reg_h     = round(float(eg["regular_hours"].sum()), 1)
            tot_h     = round(float(eg["total_hours"].sum()), 1)
            remaining = round(max(0, WEEKLY_LIMIT - tot_h), 1)
            proj      = round(tot_h / days_w * 6, 1) if days_w else tot_h
            risk      = classify(tot_h)
            # Daily breakdown
            daily = {}
            for _, row in eg.iterrows():
                k = str(row["date"])
                daily[k] = {
                    "ot":    round(float(row["ot_hours"]),1),
                    "total": round(float(row["total_hours"]),1),
                    "in":    str(row.get("in_time","")),
                    "out":   str(row.get("out_time","")),
                    "shift": str(row.get("shift","")),
                }
            emp_map[eid] = {
                **info,
                "days_worked":     days_w,
                "regular_hours":   reg_h,
                "ot_hours":        ot_h,
                "total_hours":     tot_h,
                "remaining_hours": remaining,
                "projected_hours": proj,
                "risk_level":      risk,
                "daily":           daily,
            }

        emps = sorted(emp_map.values(),
                      key=lambda e: -(risk_order(e["risk_level"])*1000 + e["total_hours"]))
        rc = defaultdict(int)
        for e in emps: rc[e["risk_level"]] += 1
        n     = len(emps)
        avg_h = round(sum(e["total_hours"] for e in emps)/n, 1) if n else 0

        curr = date.today() - timedelta(days=date.today().weekday())
        weekly[str(ws)] = {
            "week_start":      str(ws),
            "week_end":        str(ws + timedelta(days=6)),
            "is_current":      ws == curr,
            "total_employees": n,
            "avg_hours":       avg_h,
            "risk_counts":     dict(rc),
            "by_line":         group_by(emps,"line"),
            "by_section":      group_by(emps,"section"),
            "by_department":   group_by(emps,"department"),
            "by_unit":         group_by(emps,"unit"),
            "by_floor":        group_by(emps,"floor"),
            "by_team":         group_by(emps,"team"),
            "gender":          gender_split(emps),
            "employees":       emps,
        }
    return weekly

# ── MONTHLY ──────────────────────────────────────────────────────────────
def build_monthly(df):
    monthly = {}
    for m, mg in df.groupby("month"):
        emp_map = {}
        for eid, eg in mg.groupby("emp_id"):
            info  = emp_info(eg)
            ot_h  = round(float(eg["ot_hours"].sum()), 1)
            tot_h = round(float(eg["total_hours"].sum()), 1)
            days  = int(eg["date"].nunique())
            emp_map[eid] = {**info, "ot_hours":ot_h,"total_hours":tot_h,"days_worked":days}

        emps       = sorted(emp_map.values(), key=lambda e: -e["ot_hours"])
        n          = len(emps)
        total_ot   = round(sum(e["ot_hours"] for e in emps), 1)
        avg_ot     = round(total_ot/n, 1) if n else 0

        daily_trend = []
        for d, dg in mg.groupby("date"):
            daily_trend.append({
                "date":     str(d),
                "avg_ot":   round(float(dg["ot_hours"].mean()), 2),
                "total_ot": round(float(dg["ot_hours"].sum()), 1),
            })

        try:    label = datetime.strptime(m, "%Y-%m").strftime("%B %Y")
        except: label = m

        monthly[m] = {
            "month":           m,
            "label":           label,
            "total_employees": n,
            "total_ot":        total_ot,
            "avg_ot":          avg_ot,
            "top10":           emps[:10],
            "by_unit":         group_by(emps,"unit"),
            "by_floor":        group_by(emps,"floor"),
            "by_department":   group_by(emps,"department"),
            "by_line":         group_by(emps,"line"),
            "gender":          gender_split(emps),
            "daily_trend":     daily_trend,
        }
    return monthly

# ── QUARTERLY ────────────────────────────────────────────────────────────
def build_quarterly(df):
    quarterly = {}
    for q, qg in df.groupby("quarter"):
        emp_map = {}
        for eid, eg in qg.groupby("emp_id"):
            info  = emp_info(eg)
            ot_h  = round(float(eg["ot_hours"].sum()), 1)
            tot_h = round(float(eg["total_hours"].sum()), 1)
            emp_map[eid] = {**info,"ot_hours":ot_h,"total_hours":tot_h}

        emps     = sorted(emp_map.values(), key=lambda e: -e["ot_hours"])
        n        = len(emps)
        total_ot = round(sum(e["ot_hours"] for e in emps), 1)
        month_trend = []
        for mo, mg in qg.groupby("month"):
            month_trend.append({
                "month":    mo,
                "avg_ot":   round(float(mg["ot_hours"].mean()), 2),
                "total_ot": round(float(mg["ot_hours"].sum()), 1),
            })
        quarterly[q] = {
            "quarter":         q,
            "total_employees": n,
            "total_ot":        total_ot,
            "avg_ot":          round(total_ot/n,1) if n else 0,
            "top10":           emps[:10],
            "by_unit":         group_by(emps,"unit"),
            "by_floor":        group_by(emps,"floor"),
            "month_trend":     month_trend,
        }
    return quarterly

# ── YEARLY ───────────────────────────────────────────────────────────────
def build_yearly(df):
    yearly = {}
    for y, yg in df.groupby("year"):
        emp_map = {}
        for eid, eg in yg.groupby("emp_id"):
            info  = emp_info(eg)
            ot_h  = round(float(eg["ot_hours"].sum()), 1)
            emp_map[eid] = {**info,"ot_hours":ot_h}

        emps     = sorted(emp_map.values(), key=lambda e: -e["ot_hours"])
        n        = len(emps)
        total_ot = round(sum(e["ot_hours"] for e in emps), 1)
        month_trend = []
        for mo, mg in yg.groupby("month"):
            try: lbl = datetime.strptime(mo, "%Y-%m").strftime("%b")
            except: lbl = mo
            month_trend.append({
                "month": mo, "label": lbl,
                "avg_ot":   round(float(mg["ot_hours"].mean()),2),
                "total_ot": round(float(mg["ot_hours"].sum()),1),
            })
        quarterly_trend = []
        for q, qg2 in yg.groupby("quarter"):
            quarterly_trend.append({
                "quarter":  q,
                "avg_ot":   round(float(qg2["ot_hours"].mean()),2),
                "total_ot": round(float(qg2["ot_hours"].sum()),1),
            })
        yearly[y] = {
            "year":            y,
            "total_employees": n,
            "total_ot":        total_ot,
            "avg_ot":          round(total_ot/n,1) if n else 0,
            "top10":           emps[:10],
            "by_unit":         group_by(emps,"unit"),
            "by_floor":        group_by(emps,"floor"),
            "gender":          gender_split(emps),
            "month_trend":     month_trend,
            "quarterly_trend": quarterly_trend,
        }
    return yearly

# ── FORECAST ────────────────────────────────────────────────────────────
def build_forecast(df):
    today     = date.today()
    curr_week = week_start(today)
    week_df   = df[df["week"] == curr_week]
    if week_df.empty:
        # Use latest available week
        curr_week = df["week"].max()
        week_df   = df[df["week"] == curr_week]

    days_elapsed   = (today - curr_week).days + 1
    days_remaining = max(0, 6 - days_elapsed)
    at_risk = []

    for eid, eg in week_df.groupby("emp_id"):
        info      = emp_info(eg)
        days_w    = int(eg["date"].nunique())
        tot_h     = round(float(eg["total_hours"].sum()), 1)
        ot_h      = round(float(eg["ot_hours"].sum()), 1)
        remaining = round(max(0, WEEKLY_LIMIT - tot_h), 1)
        avg_daily = tot_h / days_w if days_w else tot_h
        proj      = round(avg_daily * 6, 1)
        max_daily_allowed = round(remaining / days_remaining, 1) if days_remaining else 0

        at_risk.append({
            **info,
            "current_total":        tot_h,
            "current_ot":           ot_h,
            "days_worked":          days_w,
            "days_remaining":       days_remaining,
            "projected_total":      proj,
            "remaining_allowed":    remaining,
            "max_daily_ot_allowed": max_daily_allowed,
            "avg_daily_ot":         round(ot_h/days_w,1) if days_w else 0,
            "risk_now":             classify(tot_h),
            "risk_projected":       classify(proj),
            "will_exceed":          proj >= WEEKLY_LIMIT,
        })

    at_risk.sort(key=lambda e: -e["projected_total"])
    return {
        "week_start":        str(curr_week),
        "week_end":          str(curr_week + timedelta(days=6)),
        "today":             str(today),
        "days_elapsed":      days_elapsed,
        "days_remaining":    days_remaining,
        "employees":         at_risk,
        "will_exceed_count": sum(1 for e in at_risk if e["will_exceed"]),
        "critical_count":    sum(1 for e in at_risk if e["risk_projected"] in ("critical","exceeded")),
    }

# ── SUMMARY ─────────────────────────────────────────────────────────────
def build_summary(df):
    return {
        "total_records":   len(df),
        "total_employees": df["emp_id"].nunique(),
        "total_ot_hours":  round(float(df["ot_hours"].sum()), 1),
        "avg_daily_ot":    round(float(df.groupby(["date","emp_id"])["ot_hours"].sum().mean()), 2),
        "data_from":       str(df["date"].min()),
        "data_to":         str(df["date"].max()),
        "units":           sorted(df["unit"].dropna().unique().tolist()),
        "floors":          sorted(df["floor"].dropna().unique().tolist()),
        "departments":     sorted(df["department"].dropna().unique().tolist()),
    }

# ── MAIN ─────────────────────────────────────────────────────────────────
def main():
    print("="*58)
    print("  OT Control Processor")
    print("="*58)
    print(f"\nScanning {DATA_DIR}/...")

    raw = load_all()
    if raw.empty:
        output = {
            "generated_at": datetime.now().isoformat(),
            "ot_limit": WEEKLY_LIMIT,
            "summary": {}, "forecast": {},
            "daily": {}, "weekly": {}, "monthly": {},
            "quarterly": {}, "yearly": {},
            "risk_meta": {r[0]:{"color":r[2],"bg":r[3]} for r in RISK_CFG},
        }
    else:
        df = normalise(raw)
        if df.empty:
            print("  No valid data after normalisation.")
            return
        print()
        print("  Building aggregations...")
        daily     = build_daily(df)
        weekly    = build_weekly(df)
        monthly   = build_monthly(df)
        quarterly = build_quarterly(df)
        yearly    = build_yearly(df)
        forecast  = build_forecast(df)
        summary   = build_summary(df)
        print(f"  Daily: {len(daily)} days")
        print(f"  Weekly: {len(weekly)} weeks")
        print(f"  Monthly: {len(monthly)} months")
        print(f"  Forecast: {forecast['will_exceed_count']} employees projected to exceed")

        output = {
            "generated_at": datetime.now().isoformat(),
            "ot_limit":     WEEKLY_LIMIT,
            "summary":      summary,
            "forecast":     forecast,
            "daily":        daily,
            "weekly":       weekly,
            "monthly":      monthly,
            "quarterly":    quarterly,
            "yearly":       yearly,
            "risk_meta":    {r[0]:{"color":r[2],"bg":r[3]} for r in RISK_CFG},
        }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    # ── Sanitize: replace any NaN/inf that slipped through with 0 ──
    output = sanitize(output)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, separators=(",",":"), default=str)
    kb = os.path.getsize(OUTPUT_PATH)/1024
    print(f"\n✅ Written: {OUTPUT_PATH} ({kb:.0f} KB)")
    print("="*58)

if __name__ == "__main__":
    main()
