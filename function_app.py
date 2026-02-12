import os
import re
import json
import time
import math
import logging
import requests
from urllib.parse import urljoin
from datetime import datetime, timedelta, timezone

import azure.functions as func
from dateutil import parser
from azure.storage.blob import BlobClient

app = func.FunctionApp()

# =========================================================
# CONFIG (Environment variables)
# =========================================================
PROGRAM_ID = os.getenv("PROGRAM_ID")
API_BASE_121 = os.getenv("API_BASE_121")          # must include /api
USERNAME_121 = os.getenv("USERNAME_121")
PASSWORD_121 = os.getenv("PASSWORD_121")

KOBO_BASE = os.getenv("KOBO_BASE")
KOBO_TOKEN = os.getenv("KOBO_TOKEN")
KOBO_ASSET_ID = os.getenv("KOBO_ASSET_ID")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "60"))
POST_BATCH_SIZE = int(os.getenv("POST_BATCH_SIZE", "25"))
SLEEP_BETWEEN_CALLS = float(os.getenv("SLEEP_BETWEEN_CALLS", "0.2"))

STATE_BLOB_CONNSTR = os.getenv("STATE_BLOB_CONNSTR")
STATE_CONTAINER = os.getenv("STATE_CONTAINER", "sync-state")
STATE_BLOB_NAME = os.getenv("STATE_BLOB_NAME", "sync_state.json")

if not all([
    PROGRAM_ID, API_BASE_121, USERNAME_121, PASSWORD_121,
    KOBO_BASE, KOBO_TOKEN, KOBO_ASSET_ID
]):
    raise RuntimeError("Missing required environment variables")

HEADERS_KOBO = {"Authorization": f"Token {KOBO_TOKEN}"}

# =========================================================
# HELPERS
# =========================================================
def norm_phone(x):
    if x is None:
        return ""
    s = str(x).strip()
    if not s:
        return ""
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "")
    return s


def to_int(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        return int(float(str(v)))
    except Exception:
        return None


# =========================================================
# STATE (Azure Blob)
# =========================================================
def load_state():
    if not STATE_BLOB_CONNSTR:
        return {"permanent_failures": {}}

    try:
        bc = BlobClient.from_connection_string(
            STATE_BLOB_CONNSTR,
            STATE_CONTAINER,
            STATE_BLOB_NAME,
        )
        return json.loads(bc.download_blob().readall())
    except Exception:
        return {"permanent_failures": {}}


def save_state(state):
    if not STATE_BLOB_CONNSTR:
        return

    bc = BlobClient.from_connection_string(
        STATE_BLOB_CONNSTR,
        STATE_CONTAINER,
        STATE_BLOB_NAME,
    )
    bc.upload_blob(
        json.dumps(state, indent=2).encode("utf-8"),
        overwrite=True
    )


# =========================================================
# 121 API
# =========================================================
def login_121():
    s = requests.Session()
    r = s.post(
        urljoin(API_BASE_121.rstrip("/") + "/", "users/login"),
        json={"username": USERNAME_121, "password": PASSWORD_121},
        timeout=60
    )
    r.raise_for_status()
    token = r.json().get("access_token_general")
    if not token:
        raise RuntimeError("121 login failed: token missing")
    s.cookies.set("access_token_general", token)
    return s


def get_all_121_reference_ids(session, limit=2000):
    url = urljoin(API_BASE_121.rstrip("/") + "/", f"programs/{PROGRAM_ID}/registrations")
    r = session.get(url, params={"page": 1, "limit": limit, "sortBy": "id:ASC"}, timeout=180)
    r.raise_for_status()
    payload = r.json()
    total_pages = payload["meta"]["totalPages"]

    refs = set(
        str(x["referenceId"])
        for x in payload["data"]
        if x.get("referenceId")
    )

    for page in range(2, total_pages + 1):
        r = session.get(url, params={"page": page, "limit": limit, "sortBy": "id:ASC"}, timeout=180)
        r.raise_for_status()
        for x in r.json()["data"]:
            if x.get("referenceId"):
                refs.add(str(x["referenceId"]))

    return refs


def get_121_phone_sets_since(session, since_dt, limit=1000):
    url = urljoin(API_BASE_121.rstrip("/") + "/", f"programs/{PROGRAM_ID}/registrations")

    phones = set()
    whatsapps = set()
    page = 1

    while True:
        r = session.get(
            url,
            params={"page": page, "limit": limit, "sortBy": "created:DESC"},
            timeout=180
        )
        r.raise_for_status()
        batch = r.json()["data"]
        if not batch:
            break

        stop = False
        for item in batch:
            created = parser.isoparse(item["created"])
            if created < since_dt:
                stop = True
                break

            p = norm_phone(item.get("phoneNumber"))
            w = norm_phone(item.get("whatsappPhoneNumber"))
            if p:
                phones.add(p)
            if w:
                whatsapps.add(w)

        if stop:
            break
        page += 1

    return phones, whatsapps


# =========================================================
# KOBO
# =========================================================
def get_kobo_submissions_since(since_iso, fields, page_size=300):
    url = f"{KOBO_BASE.rstrip('/')}/api/v2/assets/{KOBO_ASSET_ID}/data.json"
    results = []
    start = 0
    query = {"_submission_time": {"$gte": since_iso}}

    while True:
        params = {
            "query": json.dumps(query),
            "start": start,
            "limit": page_size,
            "fields": json.dumps(fields),
        }
        r = requests.get(url, headers=HEADERS_KOBO, params=params, timeout=180)
        r.raise_for_status()
        batch = r.json()["results"]
        results.extend(batch)
        if len(batch) < page_size:
            break
        start += page_size

    return results


# =========================================================
# MAPPING
# =========================================================
KoboTo121 = {
    "referenceId": "_uuid",
    "fullName": "algemeen/naam",
    "phoneNumber": "algemeen/telefoonNummerFinal",
    "whatsappPhoneNumber": "structureleBetaling/whatsappNummer",
    "fspName": "fsp",
    "programFspConfigurationName": "fsp",
    "regio": "regio",
    "scope": "scope",
    "namePartnerOrganization": "partner",
    "contactPerson": "routeGroep/interviewer",
    "rodeKruisType": "routeGroep/rodeKruisType",
    "amountAdultsHH": "algemeen/volwassenen",
    "amountChildrenHH": "algemeen/kinderen",
    "paymentAmountMultiplier": "algemeen/multiplier",
    "maxPayments": "maxPayments",
}

INT_FIELDS = {
    "amountAdultsHH",
    "amountChildrenHH",
    "paymentAmountMultiplier",
    "maxPayments",
}


def build_payload(sub):
    p = {}
    for k121, kkobo in KoboTo121.items():
        v = sub.get(kkobo)
        if v not in (None, ""):
            p[k121] = v

    if "scope" in p:
        p["scope"] = p["scope"].lower()

    for k in INT_FIELDS:
        if k in p:
            iv = to_int(p[k])
            if iv is None:
                p.pop(k)
            else:
                p[k] = iv

    return p


# =========================================================
# POSTING
# =========================================================
def post_batch(session, payloads):
    url = urljoin(API_BASE_121.rstrip("/") + "/", f"programs/{PROGRAM_ID}/registrations")
    return session.post(url, json=payloads, timeout=180)


def post_with_split(session, payloads):
    r = post_batch(session, payloads)
    if r.status_code in (200, 201):
        return payloads, []

    if len(payloads) == 1:
        return [], [{
            "referenceId": payloads[0].get("referenceId"),
            "error": r.text[:2000]
        }]

    mid = len(payloads) // 2
    ok1, bad1 = post_with_split(session, payloads[:mid])
    time.sleep(SLEEP_BETWEEN_CALLS)
    ok2, bad2 = post_with_split(session, payloads[mid:])
    return ok1 + ok2, bad1 + bad2


# =========================================================
# TIMER ENTRYPOINT
# =========================================================
@app.timer_trigger(
    schedule="0 0 2 * * *",  # daily 02:00 UTC
    arg_name="timer",
    run_on_startup=False,
)
def daily_sync(timer: func.TimerRequest):
    logging.info("=== Kobo → 121 daily sync start ===")

    state = load_state()
    permanent_failures = state.get("permanent_failures", {})

    since_dt = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    since_iso = since_dt.isoformat().replace("+00:00", "Z")

    session = login_121()

    ref_ids = get_all_121_reference_ids(session)
    phone_set, wa_set = get_121_phone_sets_since(session, since_dt)

    fields = ["_uuid", "_submission_time"] + list(KoboTo121.values())
    subs = get_kobo_submissions_since(since_iso, fields)

    candidates = []
    for s in subs:
        uuid = s.get("_uuid")
        if not uuid:
            continue
        if uuid in ref_ids:
            continue
        if uuid in permanent_failures:
            continue
        candidates.append(s)

    dedup = {}
    for s in candidates:
        name = (s.get("algemeen/naam") or "").lower().strip()
        phone = norm_phone(s.get("algemeen/telefoonNummerFinal"))
        key = f"{name}|{phone}"
        t = parser.isoparse(s["_submission_time"])
        if key not in dedup or t > parser.isoparse(dedup[key]["_submission_time"]):
            dedup[key] = s

    final = []
    for s in dedup.values():
        if norm_phone(s.get("algemeen/telefoonNummerFinal")) in phone_set:
            continue
        if norm_phone(s.get("structureleBetaling/whatsappNummer")) in wa_set:
            continue
        final.append(s)

    payloads = [build_payload(s) for s in final if s.get("_uuid")]

    successes = []
    failures = []

    for i in range(0, len(payloads), POST_BATCH_SIZE):
        ok, bad = post_with_split(session, payloads[i:i+POST_BATCH_SIZE])
        successes.extend(ok)
        failures.extend(bad)

    now = datetime.now(timezone.utc).isoformat()
    for f in failures:
        rid = f.get("referenceId")
        if not rid:
            continue
        permanent_failures[rid] = {
            "last_error": f.get("error"),
            "last_attempt": now,
        }

    state["permanent_failures"] = permanent_failures
    save_state(state)

    logging.info(
        f"Done. attempted={len(payloads)} "
        f"success={len(successes)} failed={len(failures)}"
    )
    logging.info("=== Kobo → 121 daily sync end ===")
