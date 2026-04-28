"""
OT Control Management — Comprehensive Processor
Reads Excel/CSV from /data/, outputs full JSON for the dashboard.
Supports: daily, weekly, monthly, quarterly, yearly views + forecasting.
"""
import os, json, glob, re
from datetime import date, datetime, timedelta
from collections import defaultdict
import pandas as pd

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

def week_start(d):
    return d - timedelta(days=d.weekday())

def quarter_of(d):
    return f"{d.year}-Q{(d.month-1)//3+1}"

# ── Load All Files ──────────────────────────────────────────────
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
        print("  No data files found.")
        return pd.DataFrame()

    frames = []
    for fpath in files:
        try:
            ext = fpath.lower().split(".")[-1]
            if ext in ("xlsx", "xls"):
                df = pd.read_excel(fpath, header=1, dtype=str)
            else:
                df = pd.read_csv(fpath, dtype=str, encoding="utf-8-sig")

            df.columns = [
                re.sub(r'[\n\r]+', ' ', str(c)).strip().lower()
                .replace(' ', '_').replace('(','').replace(')','')
                for c in df.columns
            ]
            df["_src"] = os.path.basename(fpath)
            frames.append(df)
            print(f"  Loaded {len(df):>5} rows — {os.path.basename(fpath)}")
        except Exception as e:
            print(f"  SKIP {os.path.basename(fpath)}: {e}")

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)

# ── Normalise Columns ───────────────────────────────────────────
COL_ALIASES = {
    "date":           ["date"],
    "emp_id":         ["employee_id","emp_id","id","empid","employee_no"],
    "emp_name":       ["employee_name","name","full_name","emp_name"],
    "department":     ["department","dept","division"],
    "line":           ["production_line","line","prod_line"],
    "section":        ["section","area","unit"],
    "regular_hours":  ["regular_hours","regular","reg_hours","reg"],
    "ot_hours":       ["ot_hours","ot","overtime","overtime_hours"],
    "total_hours":    ["total_hours","total"],
}

def resolve_col(df_cols, candidates):
    for c in candidates:
        for dc in df_cols:
            if dc == c or dc.startswith(c): return dc
    return None

def normalise(df):
    col_map = {}
    for std, cands in COL_ALIASES.items():
        found = resolve_col(df.columns.tolist(), cands)
        if found: col_map[found] = std
    df = df.rename(columns=col_map)

    required = ["date", "emp_id", "ot_hours"]
    missing  = [c for c in required if c not in df.columns]
    if missing:
        print(f"  WARNING: Missing columns {missing} — skipping file block")
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=False).dt.date
    df = df[df["date"].notna() & df["emp_id"].notna()]
    df = df[df["emp_id"].astype(str).str.strip() != ""]

    df["ot_hours"]      = pd.to_numeric(df.get("ot_hours",     0), errors="coerce").fillna(0)
    df["regular_hours"] = pd.to_numeric(df.get("regular_hours", 8), errors="coerce").fillna(8)
    df["total_hours"]   = df["regular_hours"] + df["ot_hours"]

    for col in ["emp_name","department","line","section"]:
        if col not in df.columns: df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    df["emp_id"] = df["emp_id"].astype(str).str.strip()
    df["week"]   = df["date"].apply(week_start)
    df["month"]  = df["date"].apply(lambda d: d.strftime("%Y-%m"))
    df["quarter"]= df["date"].apply(quarter_of)
    df["year"]   = df["date"].apply(lambda d: str(d.year))

    return df

# ── Employee info lookup ────────────────────────────────────────
def emp_info(grp):
    last = grp.iloc[-1]
    return {
        "emp_id":     str(last["emp_id"]),
        "emp_name":   str(last.get("emp_name","")),
        "department": str(last.get("department","")),
        "line":       str(last.get("line","")),
        "section":    str(last.get("section","")),
    }

# ── DAILY aggregation ───────────────────────────────────────────
def build_daily(df):
    daily = {}
    for d, dg in df.groupby("date"):
        emps = []
        for eid, eg in dg.groupby("emp_id"):
            info   = emp_info(eg)
            ot_h   = round(float(eg["ot_hours"].sum()), 1)
            reg_h  = round(float(eg["regular_hours"].sum()), 1)
            tot_h  = round(float(eg["total_hours"].sum()), 1)
            emps.append({**info, "ot_hours": ot_h, "regular_hours": reg_h, "total_hours": tot_h})

        emps.sort(key=lambda e: e["ot_hours"], reverse=True)
        total_ot = round(sum(e["ot_hours"] for e in emps), 1)
        avg_ot   = round(total_ot / len(emps), 1) if emps else 0

        daily[str(d)] = {
            "date":       str(d),
            "day_name":   d.strftime("%A"),
            "employees":  len(emps),
            "total_ot":   total_ot,
            "avg_ot":     avg_ot,
            "top10":      emps[:10],
            "all":        emps,
        }
    return daily

# ── WEEKLY aggregation ──────────────────────────────────────────
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
            emp_map[eid] = {
                **info,
                "days_worked":     days_w,
                "ot_hours":        ot_h,
                "regular_hours":   reg_h,
                "total_hours":     tot_h,
                "remaining_hours": remaining,
                "projected_hours": proj,
                "risk_level":      risk,
            }

        emps = sorted(emp_map.values(), key=lambda e: -(RISK_CFG_ORDER(e["risk_level"])*1000 + e["total_hours"]))
        rc   = defaultdict(int)
        for e in emps: rc[e["risk_level"]] += 1
        total_emps = len(emps)
        avg_h = round(sum(e["total_hours"] for e in emps)/total_emps, 1) if total_emps else 0

        weekly[str(ws)] = {
            "week_start":    str(ws),
            "week_end":      str(ws + timedelta(days=6)),
            "total_employees": total_emps,
            "avg_hours":     avg_h,
            "risk_counts":   dict(rc),
            "by_line":       group_by(emps, "line"),
            "by_section":    group_by(emps, "section"),
            "by_department": group_by(emps, "department"),
            "employees":     emps,
        }
    return weekly

def RISK_CFG_ORDER(name):
    m = {"exceeded":5,"critical":4,"warning":3,"caution":2,"safe":1}
    return m.get(name, 0)

def group_by(emps, field):
    groups = defaultdict(lambda: {"total_hours":0,"ot_hours":0,"employees":0,"risk_counts":defaultdict(int)})
    for e in emps:
        g = groups[e.get(field) or "—"]
        g["total_hours"] += e["total_hours"]
        g["ot_hours"]    += e["ot_hours"]
        g["employees"]   += 1
        g["risk_counts"][e.get("risk_level","safe")] += 1
    result = {}
    for name, g in sorted(groups.items()):
        result[name] = {
            "avg_hours":   round(g["total_hours"]/g["employees"], 1),
            "avg_ot":      round(g["ot_hours"]/g["employees"], 1),
            "employees":   g["employees"],
            "risk_counts": dict(g["risk_counts"]),
        }
    return result

# ── MONTHLY aggregation ─────────────────────────────────────────
def build_monthly(df):
    monthly = {}
    for m, mg in df.groupby("month"):
        emp_map = {}
        for eid, eg in mg.groupby("emp_id"):
            info  = emp_info(eg)
            ot_h  = round(float(eg["ot_hours"].sum()), 1)
            reg_h = round(float(eg["regular_hours"].sum()), 1)
            tot_h = round(float(eg["total_hours"].sum()), 1)
            days  = int(eg["date"].nunique())
            emp_map[eid] = {**info, "ot_hours": ot_h, "regular_hours": reg_h, "total_hours": tot_h, "days_worked": days}

        emps       = sorted(emp_map.values(), key=lambda e: -e["ot_hours"])
        total_emps = len(emps)
        total_ot   = round(sum(e["ot_hours"] for e in emps), 1)
        avg_ot     = round(total_ot / total_emps, 1) if total_emps else 0

        # Daily OT trend for chart
        daily_trend = []
        for d, dg in mg.groupby("date"):
            daily_trend.append({
                "date":    str(d),
                "avg_ot":  round(float(dg["ot_hours"].mean()), 2),
                "total_ot":round(float(dg["ot_hours"].sum()), 1),
            })

        # Week-level summaries within this month
        weeks_in_month = []
        for ws, wg in mg.groupby("week"):
            exceeded = sum(1 for eid, eg in wg.groupby("emp_id") if eg["total_hours"].sum() >= WEEKLY_LIMIT)
            weeks_in_month.append({"week_start": str(ws), "exceeded": exceeded})

        try:
            dt = datetime.strptime(m, "%Y-%m")
            label = dt.strftime("%B %Y")
        except:
            label = m

        monthly[m] = {
            "month":       m,
            "label":       label,
            "total_employees": total_emps,
            "total_ot":    total_ot,
            "avg_ot":      avg_ot,
            "top10":       emps[:10],
            "by_line":     group_by(emps, "line"),
            "by_department": group_by(emps, "department"),
            "daily_trend": daily_trend,
            "weeks":       weeks_in_month,
        }
    return monthly

# ── QUARTERLY aggregation ───────────────────────────────────────
def build_quarterly(df):
    quarterly = {}
    for q, qg in df.groupby("quarter"):
        emp_map = {}
        for eid, eg in qg.groupby("emp_id"):
            info  = emp_info(eg)
            ot_h  = round(float(eg["ot_hours"].sum()), 1)
            tot_h = round(float(eg["total_hours"].sum()), 1)
            days  = int(eg["date"].nunique())
            emp_map[eid] = {**info, "ot_hours": ot_h, "total_hours": tot_h, "days_worked": days}

        emps       = sorted(emp_map.values(), key=lambda e: -e["ot_hours"])
        total_emps = len(emps)
        total_ot   = round(sum(e["ot_hours"] for e in emps), 1)
        avg_ot     = round(total_ot / total_emps, 1) if total_emps else 0

        # Monthly trend within quarter
        month_trend = []
        for m, mg in qg.groupby("month"):
            month_trend.append({
                "month":    m,
                "avg_ot":   round(float(mg["ot_hours"].mean()), 2),
                "total_ot": round(float(mg["ot_hours"].sum()), 1),
            })

        quarterly[q] = {
            "quarter":    q,
            "total_employees": total_emps,
            "total_ot":   total_ot,
            "avg_ot":     avg_ot,
            "top10":      emps[:10],
            "by_line":    group_by(emps, "line"),
            "month_trend": month_trend,
        }
    return quarterly

# ── YEARLY aggregation ──────────────────────────────────────────
def build_yearly(df):
    yearly = {}
    for y, yg in df.groupby("year"):
        emp_map = {}
        for eid, eg in yg.groupby("emp_id"):
            info  = emp_info(eg)
            ot_h  = round(float(eg["ot_hours"].sum()), 1)
            tot_h = round(float(eg["total_hours"].sum()), 1)
            emp_map[eid] = {**info, "ot_hours": ot_h, "total_hours": tot_h}

        emps       = sorted(emp_map.values(), key=lambda e: -e["ot_hours"])
        total_emps = len(emps)
        total_ot   = round(sum(e["ot_hours"] for e in emps), 1)
        avg_ot     = round(total_ot / total_emps, 1) if total_emps else 0

        # Monthly trend within year
        month_trend = []
        for m, mg in yg.groupby("month"):
            try: label = datetime.strptime(m, "%Y-%m").strftime("%b")
            except: label = m
            month_trend.append({
                "month": m, "label": label,
                "avg_ot":   round(float(mg["ot_hours"].mean()), 2),
                "total_ot": round(float(mg["ot_hours"].sum()), 1),
            })

        quarterly_trend = []
        for q, qg in yg.groupby("quarter"):
            quarterly_trend.append({
                "quarter":  q,
                "avg_ot":   round(float(qg["ot_hours"].mean()), 2),
                "total_ot": round(float(qg["ot_hours"].sum()), 1),
            })

        yearly[y] = {
            "year":       y,
            "total_employees": total_emps,
            "total_ot":   total_ot,
            "avg_ot":     avg_ot,
            "top10":      emps[:10],
            "by_line":    group_by(emps, "line"),
            "month_trend":     month_trend,
            "quarterly_trend": quarterly_trend,
        }
    return yearly

# ── FORECAST: who will exceed 72h this week? ────────────────────
def build_forecast(df):
    today      = date.today()
    curr_week  = week_start(today)
    week_df    = df[df["week"] == curr_week]

    if week_df.empty:
        # Use latest available week
        latest = df["week"].max()
        week_df = df[df["week"] == latest]
        curr_week = latest

    days_worked_so_far = (today - curr_week).days + 1
    days_remaining     = max(0, 6 - days_worked_so_far)  # 6-day work week
    at_risk = []

    for eid, eg in week_df.groupby("emp_id"):
        info        = emp_info(eg)
        days_w      = int(eg["date"].nunique())
        tot_h       = round(float(eg["total_hours"].sum()), 1)
        ot_h        = round(float(eg["ot_hours"].sum()), 1)
        remaining_h = round(max(0, WEEKLY_LIMIT - tot_h), 1)

        if days_w > 0:
            avg_daily_total = tot_h / days_w
            avg_daily_ot    = ot_h  / days_w
            projected_total = round(avg_daily_total * 6, 1)
            projected_ot    = round(avg_daily_ot    * 6, 1)
        else:
            projected_total = tot_h
            projected_ot    = ot_h
            avg_daily_ot    = 0

        risk_now  = classify(tot_h)
        risk_proj = classify(projected_total)

        # Max OT per day allowed before breach
        max_daily_ot_allowed = round(remaining_h / max(days_remaining, 1), 1) if days_remaining else 0

        at_risk.append({
            **info,
            "current_total":       tot_h,
            "current_ot":          ot_h,
            "days_worked":         days_w,
            "days_remaining":      days_remaining,
            "projected_total":     projected_total,
            "projected_ot":        projected_ot,
            "remaining_allowed":   remaining_h,
            "max_daily_ot_allowed": max_daily_ot_allowed,
            "avg_daily_ot":        round(avg_daily_ot, 1),
            "risk_now":            risk_now,
            "risk_projected":      risk_proj,
            "will_exceed":         projected_total >= WEEKLY_LIMIT,
        })

    at_risk.sort(key=lambda e: -(e["projected_total"]))

    return {
        "week_start":       str(curr_week),
        "week_end":         str(curr_week + timedelta(days=6)),
        "today":            str(today),
        "days_elapsed":     days_worked_so_far,
        "days_remaining":   days_remaining,
        "employees":        at_risk,
        "will_exceed_count": sum(1 for e in at_risk if e["will_exceed"]),
        "critical_count":   sum(1 for e in at_risk if e["risk_projected"] in ("critical","exceeded")),
    }

# ── TOP 10 across all days ──────────────────────────────────────
def build_top10_history(df, n=30):
    """Top 10 per day for last N days."""
    top10_hist = {}
    recent_dates = sorted(df["date"].unique())[-n:]
    for d in recent_dates:
        dg   = df[df["date"] == d]
        emps = []
        for eid, eg in dg.groupby("emp_id"):
            info = emp_info(eg)
            emps.append({**info, "ot_hours": round(float(eg["ot_hours"].sum()),1)})
        emps.sort(key=lambda e: -e["ot_hours"])
        top10_hist[str(d)] = emps[:10]
    return top10_hist

# ── SUMMARY STATS ───────────────────────────────────────────────
def build_summary(df):
    total_records   = len(df)
    total_employees = df["emp_id"].nunique()
    total_ot_hours  = round(float(df["ot_hours"].sum()), 1)
    avg_daily_ot    = round(float(df.groupby(["date","emp_id"])["ot_hours"].sum().mean()), 2)
    date_range      = f"{df['date'].min()} — {df['date'].max()}"

    # Weeks with exceeded employees
    exceeded_weeks = 0
    for ws, wg in df.groupby("week"):
        for eid, eg in wg.groupby("emp_id"):
            if eg["total_hours"].sum() >= WEEKLY_LIMIT:
                exceeded_weeks += 1
                break

    return {
        "total_records":    total_records,
        "total_employees":  total_employees,
        "total_ot_hours":   total_ot_hours,
        "avg_daily_ot":     avg_daily_ot,
        "date_range":       date_range,
        "weeks_with_exceeded": exceeded_weeks,
        "data_from":        str(df["date"].min()),
        "data_to":          str(df["date"].max()),
    }

# ── MAIN ────────────────────────────────────────────────────────
def main():
    print("="*58)
    print("  OT Control — Comprehensive Processor")
    print("="*58)

    raw = load_all()
    if raw.empty:
        output = {"generated_at": datetime.now().isoformat(), "ot_limit": WEEKLY_LIMIT,
                  "summary": {}, "daily": {}, "weekly": {}, "monthly": {},
                  "quarterly": {}, "yearly": {}, "forecast": {}, "top10_history": {},
                  "risk_meta": {r[0]:{"color":r[2],"bg":r[3]} for r in RISK_CFG}}
    else:
        df = normalise(raw)
        if df.empty:
            print("  No valid records after normalisation.")
            return

        print(f"\n  Total valid records: {len(df):,}")
        print(f"  Employees: {df['emp_id'].nunique()}")
        print(f"  Date range: {df['date'].min()} → {df['date'].max()}")
        print()

        print("  Building daily…")
        daily = build_daily(df)
        print(f"    {len(daily)} days")

        print("  Building weekly…")
        weekly = build_weekly(df)
        print(f"    {len(weekly)} weeks")

        print("  Building monthly…")
        monthly = build_monthly(df)
        print(f"    {len(monthly)} months")

        print("  Building quarterly…")
        quarterly = build_quarterly(df)
        print(f"    {len(quarterly)} quarters")

        print("  Building yearly…")
        yearly = build_yearly(df)
        print(f"    {len(yearly)} years")

        print("  Building forecast…")
        forecast = build_forecast(df)
        print(f"    {forecast['will_exceed_count']} employees projected to exceed")

        print("  Building top-10 history…")
        top10 = build_top10_history(df)

        summary = build_summary(df)
        risk_meta = {r[0]:{"color":r[2],"bg":r[3],"range":f"{r[1]}–{r[2]}h"} for r in RISK_CFG}

        output = {
            "generated_at":  datetime.now().isoformat(),
            "ot_limit":      WEEKLY_LIMIT,
            "summary":       summary,
            "forecast":      forecast,
            "daily":         daily,
            "weekly":        weekly,
            "monthly":       monthly,
            "quarterly":     quarterly,
            "yearly":        yearly,
            "top10_history": top10,
            "risk_meta":     risk_meta,
        }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, separators=(",",":"), default=str)

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"\n✅ Written: {OUTPUT_PATH} ({size_kb:.1f} KB)")
    print("="*58)

if __name__ == "__main__":
    main()
