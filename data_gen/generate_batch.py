#!/usr/bin/env python3
"""
generate_batch.py - stateful micro-batch JSON generator for the
Warranty Service Request & Repair Lifecycle platform (v2: Databricks + S3 + Airflow + dbt).

Each run simulates one (or more) "batches" of source-system activity:
  * new customers / assets / warranty entitlements
  * new customer contacts (some resolved at contact, some become service requests)
  * EXISTING open cases advance: repair events, status changes, closures
  * realism: late-arriving rows, exact duplicates, a few bad rows, repeat requests,
    SLA breach rate that drifts upward over time

Output is newline-delimited JSON (one object per line), one file per table per batch:
    <out-dir>/<table>/dt=YYYY-MM-DD/<table>_<batch_id>.json
plus a manifest per batch (written LAST, so it acts as the commit marker):
    <out-dir>/_manifests/<batch_id>.json

State (open cases, id counters, deferred late rows) lives in --state-file so that
every run continues where the last one stopped. Standard library only.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

UTC = timezone.utc
TS_FMT = "%Y-%m-%dT%H:%M:%SZ"
TABLES = [
    "customers",
    "product_assets",
    "warranty_entitlements",
    "customer_contacts",
    "service_requests",
    "repair_events",
    "case_closures",
]

# --------------------------------------------------------------------------- #
# Reference data
# --------------------------------------------------------------------------- #
REGIONS = {
    "North America": [
        ("US", ["Austin", "Denver", "Chicago", "Atlanta", "Seattle"]),
        ("CA", ["Toronto", "Vancouver", "Calgary"]),
    ],
    "United Kingdom": [("GB", ["London", "Manchester", "Leeds", "Bristol"])],
    "India": [("IN", ["Hyderabad", "Bengaluru", "Pune", "Chennai", "Mumbai"])],
}
REGION_WEIGHTS = {"North America": 0.45, "United Kingdom": 0.20, "India": 0.35}
REGION_CODE = {"North America": "NA", "United Kingdom": "UK", "India": "IN"}
REPAIR_CENTERS = {
    "North America": ["RC-DAL", "RC-TOR"],
    "United Kingdom": ["RC-MAN"],
    "India": ["RC-HYD", "RC-BLR"],
}
PHONE_CODE = {"US": "1", "CA": "1", "GB": "44", "IN": "91"}
FIRST = ["Aarav", "Olivia", "Liam", "Priya", "Noah", "Emma", "Rohan", "Sophia", "Ethan", "Ananya",
         "Lucas", "Mia", "Arjun", "Amelia", "Mason", "Isha", "Oliver", "Chloe", "Kabir", "Grace"]
LAST = ["Sharma", "Smith", "Patel", "Johnson", "Reddy", "Brown", "Singh", "Taylor", "Nair", "Wilson",
        "Kumar", "Davies", "Gupta", "Miller", "Rao", "Evans", "Iyer", "Clark", "Mehta", "Walker"]
SEGMENTS = {"CONSUMER": 0.7, "SMALL_BUSINESS": 0.22, "ENTERPRISE": 0.08}
PRODUCTS = [
    ("ProBook-14", "LAPTOP"), ("ProBook-16", "LAPTOP"), ("WorkBook-15", "LAPTOP"),
    ("Ultra-13", "LAPTOP"), ("Tower-T3", "DESKTOP"), ("Desktop-D5", "DESKTOP"),
    ("AllInOne-A24", "ALL_IN_ONE"),
]
PURCHASE_CHANNELS = ["ONLINE", "RETAIL", "PARTNER"]
# (plan, warranty_type, years, weight)
PLANS = [
    ("1 Year Mail-In Warranty", "MAIL_IN", 1, 0.30),
    ("1 Year Onsite Warranty", "ONSITE", 1, 0.25),
    ("2 Year Onsite Warranty", "ONSITE", 2, 0.25),
    ("3 Year Onsite Warranty", "ONSITE", 3, 0.20),
]
CHANNELS = {"VOICE_SUPPORT": 0.50, "CHAT_SUPPORT": 0.28, "FACEBOOK_SUPPORT": 0.10, "SOCIAL_MEDIA_SUPPORT": 0.12}
DURATION_SEC = {
    "VOICE_SUPPORT": (240, 2400), "CHAT_SUPPORT": (300, 1800),
    "FACEBOOK_SUPPORT": (120, 900), "SOCIAL_MEDIA_SUPPORT": (120, 900),
}
ISSUES = {
    "NO_POWER": 0.12, "NO_POST": 0.08, "NO_VIDEO": 0.07, "NO_BOOT": 0.10, "OSRI": 0.05,
    "WINDOWS_ISSUE": 0.12, "HARDWARE_FAILURE": 0.10, "BATTERY_ISSUE": 0.10, "DISPLAY_ISSUE": 0.08,
    "KEYBOARD_TOUCHPAD_ISSUE": 0.07, "NETWORK_ISSUE": 0.06, "OTHER": 0.05,
}
# chance the issue is fixed during troubleshooting (no service request)
RESOLVE_P = {
    "NO_POWER": 0.15, "NO_POST": 0.10, "NO_VIDEO": 0.15, "NO_BOOT": 0.30, "OSRI": 0.60,
    "WINDOWS_ISSUE": 0.55, "HARDWARE_FAILURE": 0.05, "BATTERY_ISSUE": 0.15, "DISPLAY_ISSUE": 0.10,
    "KEYBOARD_TOUCHPAD_ISSUE": 0.15, "NETWORK_ISSUE": 0.50, "OTHER": 0.40,
}
# chance a service request needs a replacement part
PART_P = {
    "NO_POWER": 0.60, "NO_POST": 0.70, "NO_VIDEO": 0.50, "NO_BOOT": 0.40, "OSRI": 0.0,
    "WINDOWS_ISSUE": 0.0, "HARDWARE_FAILURE": 0.85, "BATTERY_ISSUE": 0.90, "DISPLAY_ISSUE": 0.85,
    "KEYBOARD_TOUCHPAD_ISSUE": 0.80, "NETWORK_ISSUE": 0.50, "OTHER": 0.30,
}
PARTS = {
    "NO_POWER": ["PSU-100", "MB-200", "DCJACK-030"],
    "NO_POST": ["MB-200", "RAM-8G", "CPU-I5"],
    "NO_VIDEO": ["GPU-300", "LCDCBL-040", "MB-200"],
    "NO_BOOT": ["SSD-512", "MB-200", "RAM-8G"],
    "OSRI": ["SSD-512"],
    "WINDOWS_ISSUE": ["SSD-512"],
    "HARDWARE_FAILURE": ["MB-200", "FAN-050", "SSD-512", "RAM-8G"],
    "BATTERY_ISSUE": ["BATT-045", "BATT-060"],
    "DISPLAY_ISSUE": ["LCD-14", "LCD-15", "LCDCBL-040"],
    "KEYBOARD_TOUCHPAD_ISSUE": ["KBD-US", "KBD-UK", "TPAD-010"],
    "NETWORK_ISSUE": ["WLAN-AX", "ANT-020"],
    "OTHER": ["MISC-001"],
}
PRIORITY = {"P1": 0.05, "P2": 0.15, "P3": 0.50, "P4": 0.30}
# SLA = hours from SR creation to REPAIR_COMPLETED
SLA_HOURS = {
    "MAIL_IN": {"P1": 120, "P2": 168, "P3": 240, "P4": 312},
    "ONSITE": {"P1": 72, "P2": 96, "P3": 144, "P4": 192},
}
RESOLUTION = {"REPAIRED": 0.86, "UNIT_REPLACED": 0.06, "NO_FAULT_FOUND": 0.05, "BEYOND_ECONOMICAL_REPAIR": 0.03}
CSAT = {5: 0.45, 4: 0.25, 3: 0.12, 2: 0.08, 1: 0.10}


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def fmt(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime(TS_FMT)


def parse(s: str) -> datetime:
    return datetime.strptime(s, TS_FMT).replace(tzinfo=UTC)


def wchoice(rng: random.Random, mapping: dict):
    return rng.choices(list(mapping), weights=list(mapping.values()), k=1)[0]


def new_state() -> dict:
    return {
        "version": 1,
        "batch_no": 0,
        "sim_now": None,
        "counters": {k: 0 for k in ("customer", "asset", "entitlement", "contact", "sr", "repair", "closure")},
        "customers": {},
        "assets": {},
        "open_cases": [],
        "deferred": [],
        "pending_repeats": [],
    }


def load_state(path: Path) -> dict | None:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def save_state(path: Path, st: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(st, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)


# --------------------------------------------------------------------------- #
# Batch context
# --------------------------------------------------------------------------- #
class Batch:
    def __init__(self, st: dict, args: argparse.Namespace, win_start: datetime, win_end: datetime):
        self.st, self.args = st, args
        self.no = st["batch_no"]
        self.id = f"b{self.no:06d}_{win_end.strftime('%Y%m%dT%H%M%S')}"
        self.rng = random.Random(f"{args.seed}-{self.no}")
        self.win_start, self.win_end = win_start, win_end
        self.drift = min(1.5, 1.0 + args.drift * self.no)  # delays stretch over time -> more SLA breaches
        self.rows = {t: [] for t in TABLES}
        self.defects = {t: 0 for t in TABLES}
        self.dups = {t: 0 for t in TABLES}
        self.late_deferred = 0
        self.late_released = 0

    def nid(self, key: str, prefix: str) -> str:
        self.st["counters"][key] += 1
        return f"{prefix}-{self.st['counters'][key]:06d}"

    def rand_time(self) -> datetime:
        return self.win_start + (self.win_end - self.win_start) * self.rng.random()

    def land(self, table: str, row: dict) -> None:
        """Put a row into this batch's output (stamps batch_id, may corrupt it)."""
        row = dict(row)
        row["batch_id"] = self.id
        self.rows[table].append(self._maybe_corrupt(table, row))

    def emit(self, table: str, row: dict, late_ok: bool = False) -> None:
        if late_ok and self.rng.random() < self.args.late_rate:
            self.st["deferred"].append(
                {"release_batch": self.no + self.rng.randint(1, 3), "table": table, "row": row}
            )
            self.late_deferred += 1
        else:
            self.land(table, row)

    def _maybe_corrupt(self, table: str, row: dict) -> dict:
        r = self.rng
        if r.random() >= self.args.bad_rate:
            return row
        if table == "customer_contacts":
            if r.random() < 0.5:
                row["contact_id"] = None
            else:
                row["channel"] = "FAX_SUPPORT"
        elif table == "warranty_entitlements":
            row["coverage_start"], row["coverage_end"] = row["coverage_end"], row["coverage_start"]
        elif table == "service_requests":
            if r.random() < 0.5:
                row["asset_id"] = "AST-999999"
            else:
                row["sr_id"] = None
        elif table == "repair_events":
            if row.get("repair_cost") is not None:
                row["repair_cost"] = -abs(row["repair_cost"])
            else:
                row["sr_id"] = "SR-9999999"
        elif table == "case_closures":
            row["csat_score"] = 9
        elif table == "customers":
            row["email"] = "not-an-email"
        elif table == "product_assets":
            row["purchase_date"] = (self.win_end + timedelta(days=30)).date().isoformat()
        self.defects[table] += 1
        return row


# --------------------------------------------------------------------------- #
# Master data: customers, assets, entitlements
# --------------------------------------------------------------------------- #
def make_phone(r: random.Random, country: str) -> str:
    return f"+{PHONE_CODE[country]}-555-{r.randint(100, 999)}-{r.randint(1000, 9999)}"


def make_customer(b: Batch, created: datetime) -> dict:
    r = b.rng
    cid = b.nid("customer", "CUST")
    region = wchoice(r, REGION_WEIGHTS)
    country, cities = r.choice(REGIONS[region])
    first, last = r.choice(FIRST), r.choice(LAST)
    row = {
        "customer_id": cid,
        "first_name": first,
        "last_name": last,
        "email": f"{first}.{last}{r.randint(1, 999)}@example.com".lower(),
        "phone": make_phone(r, country),
        "region": region,
        "country": country,
        "city": r.choice(cities),
        "customer_segment": wchoice(r, SEGMENTS),
        "created_at": fmt(created),
        "event_ts": fmt(created),
        "updated_at": fmt(created),
    }
    b.st["customers"][cid] = row
    return row


def make_asset(b: Batch, cid: str, now: datetime, history: bool) -> tuple[dict, dict]:
    r = b.rng
    aid = b.nid("asset", "AST")
    eid = b.nid("entitlement", "ENT")
    model, family = r.choice(PRODUCTS)
    days_ago = r.randint(0, 1300) if history else r.randint(0, 30)
    purchase = (now - timedelta(days=days_ago)).date()
    plan, wtype, years, _ = r.choices(PLANS, weights=[p[3] for p in PLANS], k=1)[0]
    cov_end = purchase + timedelta(days=365 * years)
    arow = {
        "asset_id": aid,
        "customer_id": cid,
        "product_model": model,
        "product_family": family,
        "serial_number": "".join(r.choices("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", k=10)),
        "purchase_date": purchase.isoformat(),
        "purchase_channel": r.choice(PURCHASE_CHANNELS),
        "asset_status": "ACTIVE",
        "event_ts": fmt(now),
        "updated_at": fmt(now),
    }
    erow = {
        "entitlement_id": eid,
        "asset_id": aid,
        "warranty_plan": plan,
        "warranty_type": wtype,
        "coverage_years": years,
        "coverage_start": purchase.isoformat(),
        "coverage_end": cov_end.isoformat(),
        "entitlement_status": "ACTIVE" if cov_end >= now.date() else "EXPIRED",
        "event_ts": fmt(now),
        "updated_at": fmt(now),
    }
    b.st["assets"][aid] = {"customer_id": cid, "asset": arow, "ent": erow, "last_issue": None}
    return arow, erow


def add_customer_with_assets(b: Batch, when: datetime, history: bool) -> None:
    crow = make_customer(b, when)
    b.land("customers", crow)
    n_assets = 1 + (1 if b.rng.random() < 0.4 else 0)
    for _ in range(n_assets):
        arow, erow = make_asset(b, crow["customer_id"], when, history)
        b.land("product_assets", arow)
        b.land("warranty_entitlements", erow)


# --------------------------------------------------------------------------- #
# Case planning (the future timeline of one service request)
# --------------------------------------------------------------------------- #
def plan_steps(b: Batch, case: dict) -> list[dict]:
    r, f = b.rng, b.drift

    def h(lo: float, hi: float) -> timedelta:
        return timedelta(hours=r.uniform(lo, hi))

    steps: list[dict] = []

    def add(t: datetime, event_type: str | None = None, status: str | None = None, **extra) -> None:
        steps.append({"due": fmt(t), "action": "step", "event_type": event_type, "status": status, **extra})

    t0 = parse(case["created_ts"])
    steps.append({"due": case["created_ts"], "action": "sr_create"})
    part = case["part_number"]
    pn = {"part_number": part} if part else {}
    parts_cost = round(r.uniform(25, 420), 2) if part else 0.0

    if case["service_type"] == "MAIL_IN":
        t1 = t0 + h(24, 72) * f
        add(t1, "DEVICE_RECEIVED", "IN_REPAIR")
        t2 = t1 + h(4, 24) * f
        add(t2, "DIAGNOSED", **pn)
        if part:
            t3 = t2 + h(1, 4)
            eta = t3 + h(24, 120) * f
            add(t3, "PART_ORDERED", "AWAITING_PARTS", part_eta=fmt(eta), **pn)
            t4 = max(eta + h(-6, 24), t3 + timedelta(hours=12))
            add(t4, "PART_RECEIVED", "IN_REPAIR", **pn)
            t5 = t4 + h(2, 12)
        else:
            t5 = t2 + h(2, 12)
        add(t5, "REPAIR_STARTED")
        labor = round(r.uniform(1, 6), 1)
        cost = round(parts_cost + labor * r.choice([30, 35, 45]), 2)
        t6 = t5 + timedelta(hours=labor) + h(2, 20) * f
        add(t6, "REPAIR_COMPLETED", "REPAIR_COMPLETE", labor_hours=labor, repair_cost=cost, **pn)
        t7 = t6 + h(8, 36)
        add(t7, "SHIPPED_BACK")
        t_close = t7 + h(24, 96)
    else:  # ONSITE
        t1 = t0 + h(4, 36) * f
        add(t1, "TECH_DISPATCHED", "IN_REPAIR")
        t2 = t1 + h(2, 10)
        add(t2, "DIAGNOSED", **pn)
        if part:
            t3 = t2 + h(0.5, 2)
            eta = t3 + h(24, 96) * f
            add(t3, "PART_ORDERED", "AWAITING_PARTS", part_eta=fmt(eta), **pn)
            t4 = eta + h(0, 12)
            add(t4, "PART_RECEIVED", "IN_REPAIR", **pn)
            t5 = t4 + h(4, 24) * f
            add(t5, "TECH_DISPATCHED")  # second visit
            t6 = t5 + h(1, 4)
        else:
            t6 = t2 + h(1, 4)
        labor = round(r.uniform(0.5, 3), 1)
        cost = round(parts_cost + labor * r.choice([40, 50, 60]) + 60, 2)  # + trip fee
        add(t6, "REPAIR_COMPLETED", "REPAIR_COMPLETE", labor_hours=labor, repair_cost=cost, **pn)
        t_close = t6 + h(4, 48)

    steps.append({
        "due": fmt(t_close),
        "action": "closure",
        "resolution_code": wchoice(r, RESOLUTION),
        "reopened_flag": r.random() < 0.05,
        "csat_score": None if r.random() < 0.30 else wchoice(r, CSAT),
    })
    steps.sort(key=lambda s: s["due"])
    return steps


# --------------------------------------------------------------------------- #
# Contacts -> (optional) service request
# --------------------------------------------------------------------------- #
def make_contact_and_case(b: Batch, when: datetime, asset_id: str, issue: str, repeat: bool) -> None:
    r, st = b.rng, b.st
    a = st["assets"][asset_id]
    cust = st["customers"][a["customer_id"]]
    ent = a["ent"]
    in_warranty = ent["coverage_start"] <= when.date().isoformat() <= ent["coverage_end"]

    channel = wchoice(r, CHANNELS)
    lo, hi = DURATION_SEC[channel]
    resolve_p = RESOLVE_P[issue] * (0.3 if repeat else 1.0)
    if r.random() < resolve_p:
        outcome, make_sr = "RESOLVED_AT_CONTACT", False
    elif not in_warranty and r.random() < 0.65:
        outcome, make_sr = "OUT_OF_WARRANTY_DECLINED", False
    else:
        outcome, make_sr = "SERVICE_REQUEST_CREATED", True

    contact_id = b.nid("contact", "CON")
    b.land("customer_contacts", {
        "contact_id": contact_id,
        "customer_id": cust["customer_id"],
        "asset_id": asset_id,
        "channel": channel,
        "region": cust["region"],
        "country": cust["country"],
        "issue_category": issue,
        "contact_ts": fmt(when),
        "duration_sec": r.randint(lo, hi),
        "agent_id": f"AGT-{r.randint(1, 60):03d}",
        "warranty_status_at_contact": "IN_WARRANTY" if in_warranty else "OUT_OF_WARRANTY",
        "troubleshooting_outcome": outcome,
        "first_contact_resolved": outcome == "RESOLVED_AT_CONTACT",
        "event_ts": fmt(when),
        "updated_at": fmt(when),
    })
    if not make_sr:
        return

    priority = wchoice(r, PRIORITY)
    p_esc = {"P1": 0.35, "P2": 0.15}.get(priority, 0.05)
    if issue in ("HARDWARE_FAILURE", "NO_POST"):
        p_esc += 0.05
    escalated = r.random() < p_esc
    service_type = ent["warranty_type"]
    created = when + timedelta(minutes=r.uniform(5, 45))
    region = cust["region"]
    needs_part = r.random() < PART_P[issue]
    case = {
        "sr_id": b.nid("sr", "SR"),
        "contact_id": contact_id,
        "customer_id": cust["customer_id"],
        "asset_id": asset_id,
        "entitlement_id": ent["entitlement_id"],
        "region": region,
        "country": cust["country"],
        "issue_category": issue,
        "priority": priority,
        "service_type": service_type,
        "warranty_status": "IN_WARRANTY" if in_warranty else "OUT_OF_WARRANTY",
        "created_ts": fmt(created),
        "sla_due_ts": fmt(created + timedelta(hours=SLA_HOURS[service_type][priority])),
        "escalated_flag": escalated,
        "assigned_team": "ESCALATION" if escalated else r.choice(["TIER1", "TIER1", "TIER2"]),
        "part_number": r.choice(PARTS[issue]) if needs_part else None,
        "repair_center": r.choice(REPAIR_CENTERS[region]) if service_type == "MAIL_IN" else None,
        "technician_id": f"TECH-{REGION_CODE[region]}-{r.randint(1, 25):02d}",
        "status": None,
        "steps": [],
    }
    case["steps"] = plan_steps(b, case)
    st["open_cases"].append(case)
    a["last_issue"] = issue


# --------------------------------------------------------------------------- #
# Advance open cases up to the end of this batch window
# --------------------------------------------------------------------------- #
def sr_row(case: dict, status: str, ts: str, closed_ts: str | None = None) -> dict:
    return {
        "sr_id": case["sr_id"],
        "contact_id": case["contact_id"],
        "customer_id": case["customer_id"],
        "asset_id": case["asset_id"],
        "entitlement_id": case["entitlement_id"],
        "region": case["region"],
        "country": case["country"],
        "issue_category": case["issue_category"],
        "priority": case["priority"],
        "service_type": case["service_type"],
        "warranty_status_at_creation": case["warranty_status"],
        "billable_flag": case["warranty_status"] == "OUT_OF_WARRANTY",
        "status": status,
        "created_ts": case["created_ts"],
        "sla_due_ts": case["sla_due_ts"],
        "escalated_flag": case["escalated_flag"],
        "assigned_team": case["assigned_team"],
        "closed_ts": closed_ts,
        "event_ts": ts,
        "updated_at": ts,
    }


def emit_step(b: Batch, case: dict, s: dict) -> None:
    due = s["due"]
    if s["action"] == "sr_create":
        case["status"] = "NEW"
        b.emit("service_requests", sr_row(case, "NEW", due))
        return

    if s["action"] == "closure":
        case["status"] = "CLOSED"
        b.emit("case_closures", {
            "closure_id": b.nid("closure", "CLS"),
            "sr_id": case["sr_id"],
            "closed_ts": due,
            "resolution_code": s["resolution_code"],
            "reopened_flag": s["reopened_flag"],
            "csat_score": s["csat_score"],
            "event_ts": due,
            "updated_at": due,
        }, late_ok=True)
        b.emit("service_requests", sr_row(case, "CLOSED", due, closed_ts=due), late_ok=True)
        if s["reopened_flag"]:
            b.st["pending_repeats"].append({
                "due": fmt(parse(due) + timedelta(days=b.rng.uniform(2, 10))),
                "asset_id": case["asset_id"],
                "issue": case["issue_category"],
            })
        return

    if s.get("event_type"):
        b.emit("repair_events", {
            "repair_event_id": b.nid("repair", "REP"),
            "sr_id": case["sr_id"],
            "event_type": s["event_type"],
            "event_ts": due,
            "repair_center": case["repair_center"],
            "technician_id": case["technician_id"],
            "part_number": s.get("part_number"),
            "part_eta": s.get("part_eta"),
            "labor_hours": s.get("labor_hours"),
            "repair_cost": s.get("repair_cost"),
            "updated_at": due,
        }, late_ok=True)
    if s.get("status"):
        case["status"] = s["status"]
        b.emit("service_requests", sr_row(case, s["status"], due), late_ok=True)


def advance_cases(b: Batch) -> None:
    still_open = []
    for case in b.st["open_cases"]:
        steps = case["steps"]
        while steps and parse(steps[0]["due"]) <= b.win_end:
            emit_step(b, case, steps.pop(0))
        if steps:
            still_open.append(case)
    b.st["open_cases"] = still_open


# --------------------------------------------------------------------------- #
# One batch
# --------------------------------------------------------------------------- #
def run_batch(b: Batch) -> None:
    st, r, args = b.st, b.rng, b.args

    # 0. release late-arriving rows that were held back from earlier batches
    keep = []
    for d in st["deferred"]:
        if d["release_batch"] <= b.no:
            b.land(d["table"], d["row"])
            b.late_released += 1
        else:
            keep.append(d)
    st["deferred"] = keep

    # 1. customer profile updates (same customer_id, newer updated_at)
    ids = list(st["customers"])
    for cid in r.sample(ids, min(len(ids), max(1, len(ids) // 200))) if ids else []:
        c, when = st["customers"][cid], b.rand_time()
        if r.random() < 0.5:
            c["email"] = f"{c['first_name']}.{c['last_name']}{r.randint(1, 999)}@example.org".lower()
        else:
            c["phone"] = make_phone(r, c["country"])
        c["updated_at"] = c["event_ts"] = fmt(when)
        b.land("customers", c)

    # 2. master-data growth (seed population on the very first batch)
    if not st["customers"]:
        for _ in range(args.seed_customers):
            add_customer_with_assets(b, b.win_end, history=True)
    else:
        for _ in range(r.randint(0, max(1, args.contacts_per_batch // 12))):
            add_customer_with_assets(b, b.rand_time(), history=False)
        for _ in range(r.randint(0, 2)):  # extra device for an existing customer
            cid = r.choice(list(st["customers"]))
            arow, erow = make_asset(b, cid, b.rand_time(), history=False)
            b.land("product_assets", arow)
            b.land("warranty_entitlements", erow)

    # 3. warranties that expired since the last batch -> status update rows
    today = b.win_end.date().isoformat()
    for a in st["assets"].values():
        e = a["ent"]
        if e["entitlement_status"] == "ACTIVE" and e["coverage_end"] < today:
            e["entitlement_status"] = "EXPIRED"
            e["updated_at"] = e["event_ts"] = fmt(b.win_end)
            b.land("warranty_entitlements", e)

    # 4. new customer contacts (some become service requests)
    assets = st["assets"]
    all_ids = list(assets)
    active_ids = [i for i, a in assets.items() if a["ent"]["coverage_start"] <= today <= a["ent"]["coverage_end"]]
    repeat_pool = [i for i, a in assets.items() if a["last_issue"]]
    n = max(1, int(r.gauss(args.contacts_per_batch, args.contacts_per_batch * 0.2)))
    for _ in range(n):
        when = b.rand_time()
        if repeat_pool and r.random() < 0.06:  # chronic-issue repeat on the same asset
            aid = r.choice(repeat_pool)
            make_contact_and_case(b, when, aid, assets[aid]["last_issue"], repeat=True)
        else:
            aid = r.choice(active_ids if active_ids and r.random() < 0.82 else all_ids)
            make_contact_and_case(b, when, aid, wchoice(r, ISSUES), repeat=False)

    due_rep, rest = [], []
    for p in st["pending_repeats"]:
        (due_rep if parse(p["due"]) <= b.win_end else rest).append(p)
    st["pending_repeats"] = rest
    for p in due_rep:  # customer comes back after a case was reopened
        lo = max(parse(p["due"]), b.win_start)
        make_contact_and_case(b, lo + (b.win_end - lo) * r.random(), p["asset_id"], p["issue"], repeat=True)

    # 5. move existing open cases forward in time
    advance_cases(b)

    # 6. exact duplicate rows (at-least-once delivery)
    for t in TABLES:
        base = list(b.rows[t])
        for _ in range(sum(1 for _ in base if r.random() < args.dup_rate)):
            b.rows[t].append(dict(r.choice(base)))
            b.dups[t] += 1


def write_batch(b: Batch, out_dir: Path) -> dict:
    dt = b.win_end.strftime("%Y-%m-%d")
    files, counts = {}, {}
    for t, rows in b.rows.items():
        counts[t] = len(rows)
        if not rows:
            continue
        d = out_dir / t / f"dt={dt}"
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{t}_{b.id}.json"
        tmp = d / f".{path.name}.tmp"  # dot-prefixed temp name: listers should skip it
        with open(tmp, "w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
        os.replace(tmp, path)
        files[t] = str(path.relative_to(out_dir))

    manifest = {
        "batch_id": b.id,
        "batch_no": b.no,
        "window_start": fmt(b.win_start),
        "window_end": fmt(b.win_end),
        "rows": counts,
        "files": files,
        "injected_bad_rows": {t: n for t, n in b.defects.items() if n},
        "injected_duplicates": {t: n for t, n in b.dups.items() if n},
        "late_rows_deferred_to_future_batches": b.late_deferred,
        "late_rows_released_in_this_batch": b.late_released,
        "open_cases_after_batch": len(b.st["open_cases"]),
        "sla_delay_factor": round(b.drift, 3),
    }
    mdir = out_dir / "_manifests"
    mdir.mkdir(parents=True, exist_ok=True)
    mtmp = mdir / f".{b.id}.json.tmp"
    mtmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    os.replace(mtmp, mdir / f"{b.id}.json")  # written last = commit marker
    return manifest


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Micro-batch generator for the warranty lifecycle JSON sources.")
    p.add_argument("--out-dir", default="./output", help="where batch files are written")
    p.add_argument("--state-file", default="./state/state.json", help="generator state (open cases, counters)")
    p.add_argument("--batches", type=int, default=1, help="how many batches to generate in this run")
    p.add_argument("--tick-hours", type=float, default=4.0, help="simulated hours covered by one batch (sim clock)")
    p.add_argument("--clock", choices=["sim", "real"], default="sim",
                   help="sim: each batch advances a simulated clock; real: window ends at wall-clock now")
    p.add_argument("--start", default=None, help="sim clock start, e.g. 2026-10-20T00:00:00Z (default: now, hour floor)")
    p.add_argument("--contacts-per-batch", type=int, default=60, help="average new customer contacts per batch")
    p.add_argument("--seed-customers", type=int, default=600, help="customers created in the very first batch")
    p.add_argument("--seed", type=int, default=42, help="random seed (same seed + same state = same output)")
    p.add_argument("--late-rate", type=float, default=0.03, help="share of event rows delivered 1-3 batches late")
    p.add_argument("--dup-rate", type=float, default=0.02, help="share of rows delivered twice")
    p.add_argument("--bad-rate", type=float, default=0.01, help="share of rows with an injected defect")
    p.add_argument("--drift", type=float, default=0.0015, help="per-batch growth of repair delays (SLA breach drift)")
    p.add_argument("--sleep-seconds", type=float, default=0.0, help="pause between batches when --batches > 1")
    p.add_argument("--reset", action="store_true", help="delete the state file first (start a fresh simulation)")
    args = p.parse_args(argv)

    out_dir, state_path = Path(args.out_dir), Path(args.state_file)
    if args.reset and state_path.exists():
        state_path.unlink()

    st = load_state(state_path)
    if st is None:
        st = new_state()
        if args.start:
            start = parse(args.start)
        else:
            start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        st["sim_now"] = fmt(start - timedelta(hours=args.tick_hours))

    for i in range(args.batches):
        prev = parse(st["sim_now"])
        if args.clock == "real":
            win_end = max(datetime.now(UTC).replace(microsecond=0), prev + timedelta(seconds=1))
        else:
            win_end = prev + timedelta(hours=args.tick_hours)
        st["batch_no"] += 1
        b = Batch(st, args, prev, win_end)
        run_batch(b)
        manifest = write_batch(b, out_dir)
        st["sim_now"] = fmt(win_end)
        save_state(state_path, st)  # after files: a crash in between just regenerates the same batch id
        print(json.dumps({
            "batch_id": manifest["batch_id"],
            "window_end": manifest["window_end"],
            "rows": manifest["rows"],
            "late_deferred": manifest["late_rows_deferred_to_future_batches"],
            "late_released": manifest["late_rows_released_in_this_batch"],
            "open_cases": manifest["open_cases_after_batch"],
        }))
        if args.sleep_seconds and i < args.batches - 1:
            time.sleep(args.sleep_seconds)
    return 0


if __name__ == "__main__":
    sys.exit(main())