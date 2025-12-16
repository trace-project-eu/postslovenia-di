"""
Standalone ingestor  that, every 5 minutes, reads barcodes from
`shipments.txt`, fetches data from TT API and CRN API (both OAuth-protected),
then writes results to your REST-style Mongo API wrapper at:
  - POST/GET /api/v1/shipments   (insert ONLY: destination, shipmentExternalID)
  - POST/GET /api/v1/points      (insert: lat, lon, belongsTo)

Logic:
- For each barcode in shipments.txt:
  - Check if shipment already exists via GET /shipments?shipmentExternalID=<id>
  - If exists → skip (no TT/CRN calls, no inserts)
  - If not →
      - TT: parcels.select.barcode → take Postalname (origin) and Note
      - CRN: crn.select(HouseId=Note) → take Town (destination), Latitude/Longitude
      - Insert shipment with ONLY { destination, shipmentExternalID }
      - Insert point with { lat, lon, belongsTo=barcode } if lat/lon are present

Run (Python 3.12+):
  pip install httpx tenacity
  python post_slovenia/data_ingestor.py

All constants are hardcoded below, as requested.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ---------------------- Hardcoded Constants ----------------------
BASIC_USER = "99BD6E49-382D-41FB-AFB3-8A86914296C4"
BASIC_PASS = "9C60807D-7986-4157-BEFF-95F0915DF2CD"
USER_TOKEN = "a07ff43f-5aff-4b13-9748-b1bc7cd90596"
# TT API (Tracking)
TT_OAUTH_URL = "https://oauth.appservice.azuremb.posta.si/api"
TT_BASE_URL = "https://tracking.appservice.azuremb.posta.si/api"

# CRN API
CRN_OAUTH_URL = "https://oauth.appservice.azuremb.posta.si/api"
CRN_BASE_URL = "https://crn.appservice.azuremb.posta.si/api"
CRN_ECRN_TOKEN = "3C9EC6C5"

# REST-style Mongo API wrapper base
MONGO_API_BASE = "http://142.132.165.122:9014/api/v1"  # base URL for the API wrapper (REST style)
MONGO_SHIPMENTS_URL = f"{MONGO_API_BASE.rstrip('/')}/shipments"
MONGO_POINTS_URL = f"{MONGO_API_BASE.rstrip('/')}/points"

# Optional auth header for the wrapper (set to None if not required)
MONGO_AUTH_HEADER_KEY = None           # e.g., "Authorization" or "X-API-Key"
MONGO_AUTH_HEADER_VALUE = None         # e.g., "Bearer <token>" or API key

# Local file & schedule
SHIPMENTS_FILE = "shipments.txt"
POLL_SECONDS = 900  # 1:30 minutes

# ---------------------- Logging ----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingestor")

# ---------------------- HTTP Client ----------------------
http_timeout = httpx.Timeout(30.0, connect=15.0)
client = httpx.AsyncClient(timeout=http_timeout)

# ---------------------- Errors ----------------------
class OAuthError(Exception):
    pass

class UpstreamError(Exception):
    pass

class MongoAPIError(Exception):
    pass

# ---------------------- Helpers ----------------------

def _basic_auth_header(user: str, password: str) -> str:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return f"Basic {token}"


def _first_tokenish_value(payload: Any) -> Optional[str]:
    if isinstance(payload, str) and payload.count('.') >= 2:
        return payload
    if isinstance(payload, dict):
        for key in ("access_token", "token", "jwt", "id_token", "authToken", "Authorization"):
            val = payload.get(key)
            if isinstance(val, str):
                return val
        for val in payload.values():
            if isinstance(val, str):
                return val
    if isinstance(payload, list) and payload and isinstance(payload[0], str):
        return payload[0]
    return None

# ---------------------- OAuth ----------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), reraise=True,
       retry=retry_if_exception_type((httpx.HTTPError, OAuthError)))
async def fetch_oauth_token(oauth_url: str, basic_user: str, basic_pass: str, user_token: str) -> str:
    headers = {"Authorization": _basic_auth_header(basic_user, basic_pass), "Content-Type": "application/json", "Accept": "application/json"}
    try:
        logger.info("OAuth: POST %s oauth.token", oauth_url)
        logger.debug("OAuth POST body=%s headers=%s", user_token, headers)
        r = await client.post(f"{oauth_url.rstrip('/')}/oauth.token", content=json.dumps(user_token), headers=headers)
        r.raise_for_status()
    except httpx.HTTPStatusError:
        logger.info("OAuth: retry with wrapped body")
        r = await client.post(f"{oauth_url.rstrip('/')}/oauth.token", json={"token": user_token}, headers=headers)
        r.raise_for_status()
    try:
        data = r.json()
    except json.JSONDecodeError:
        data = r.text
    logger.info("OAuth response: %s", data)
    token = _first_tokenish_value(data)
    if not token:
        raise OAuthError(f"Could not parse token from response: {data!r}")
    return token

# ---------------------- TT / CRN calls ----------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), reraise=True,
       retry=retry_if_exception_type((httpx.HTTPError, UpstreamError)))
async def tt_select_by_barcode(token: str, barcode: str) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}
    url = f"{TT_BASE_URL.rstrip('/')}/parcels.select.barcode"
    logger.info("TT POST %s", url)
    logger.debug("TT POST body=%s headers=%s", barcode, headers)
    r = await client.post(url, content=json.dumps(barcode), headers=headers)
    r.raise_for_status()
    try:
        data = r.json()
    except json.JSONDecodeError:
        raise UpstreamError("TT API returned non-JSON body")
    logger.info("TT response for barcode=%s: %s", barcode, data)
    return data

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), reraise=True,
       retry=retry_if_exception_type((httpx.HTTPError, UpstreamError)))
async def crn_get_address(token: str, house_id: str) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}
    payload = {"Token": CRN_ECRN_TOKEN, "HouseId": house_id}
    url = f"{CRN_BASE_URL.rstrip('/')}/crn.select"
    logger.info("CRN POST %s", url)
    logger.debug("CRN POST body=%s headers=%s", payload, headers)
    r = await client.post(url, json=payload, headers=headers)
    r.raise_for_status()
    try:
        data = r.json()
    except json.JSONDecodeError:
        raise UpstreamError("CRN API returned non-JSON body")
    logger.info("CRN response for HouseId=%s: %s", house_id, data)
    return data

# ---------------------- REST Mongo wrapper helpers ----------------------

def _mongo_headers() -> Dict[str, str]:
    h = {"Accept": "application/json", "Content-Type": "application/json"}
    if MONGO_AUTH_HEADER_KEY and MONGO_AUTH_HEADER_VALUE:
        h[MONGO_AUTH_HEADER_KEY] = MONGO_AUTH_HEADER_VALUE
    return h

async def _try_http_variants(variants: list[dict]) -> httpx.Response:
    last_exc: Exception | None = None
    for v in variants:
        method = v.get("method", "GET").upper()
        url = v["url"]
        json_body = v.get("json")
        params = v.get("params")
        headers = _mongo_headers()
        try:
            logger.info("MongoAPI %s %s json=%s params=%s headers=%s", method, url, json_body, params, headers)
            r = await client.request(method, url, json=json_body, params=params, headers=headers)
            if 200 <= r.status_code < 300:
                logger.info("Success variant is: %s ", v)
                logger.info("MongoAPI success: %s %s -> %s", method, url, r.status_code)
                return r
            else:
                logger.warning("MongoAPI non-2xx: %s %s -> %s body=%s", method, url, r.status_code, r.text)
        except Exception as e:  # noqa: BLE001
            last_exc = e
            logger.exception("MongoAPI request failed for %s %s", method, url)
    if last_exc:
        raise last_exc
    raise MongoAPIError("All Mongo API variants failed with non-2xx status")

# Normalizer for list responses

def _normalize_list_response(data: Any) -> list[dict]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("documents", "results", "data", "items"):
            val = data.get(key)
            if isinstance(val, list):
                return val
        if data:
            return [data]
    return []

# ---- Shipments REST helpers ----
async def shipments_find_by_external_id(external_id: str) -> Optional[Dict[str, Any]]:
    '''variants = [
        #{"method": "GET", "url": MONGO_SHIPMENTS_URL, "params": {"shipmentExternalID": external_id}},
        {"method": "GET", "url": MONGO_SHIPMENTS_URL, "params": {"filter": json.dumps({"shipmentExternalID": external_id})}},
    ]
    try:
        r = await _try_http_variants(variants)
        try:
            data = r.json()
        except json.JSONDecodeError:
            data = r.text
        logger.info("Shipments filtered GET response: %s", data)
        docs = _normalize_list_response(data)
        for d in docs:
            if isinstance(d, dict) and d.get("shipmentExternalID") == external_id:
                return d
    except Exception:
        logger.exception("Shipments filtered GET failed; attempting full GET")'''

    r_all = await _try_http_variants([{"method": "GET", "url": MONGO_SHIPMENTS_URL}])
    try:
        data_all = r_all.json()
    except json.JSONDecodeError:
        data_all = r_all.text
    logger.info("Shipments full list response: %s", data_all)
    docs_all = _normalize_list_response(data_all)
    for d in docs_all:
        if isinstance(d, dict) and d.get("shipmentExternalID") == external_id:
            return d
    return None


def _build_shipment_payload(document: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build payload for /shipments with required defaults:
      - origin:            default "N/A"
      - shipmentWeight:    default 0
      - shipmentVolume:    default 0
      - shipmentType:      default "Parcel"
      - priority:          default 1
      - respLogisticCo:    default "Post of Slovenia"
    Also include:
      - destination (from CRN Town, or "")
      - shipmentExternalID (barcode)
      - pickUpDate, deliveryDate (from TT EventDate if available)
    Only these fields are sent.
    """
    payload: Dict[str, Any] = {
        "destination": document.get("destination") or "",
        "shipmentExternalID": document.get("shipmentExternalID"),
        "origin": document.get("origin", "N/A"),
        "shipmentWeight": document.get("shipmentWeight", 0),
        "shipmentVolume": document.get("shipmentVolume", 0),
        "shipmentType": document.get("shipmentType", "Parcel"),
        "priority": document.get("priority", 1),
        "respLogisticCo": document.get("respLogisticCo", "Post of Slovenia"),
    }
    if document.get("pickUpDate"):
        payload["pickUpDate"] = document["pickUpDate"]
    if document.get("deliveryDate"):
        payload["deliveryDate"] = document["deliveryDate"]
    return payload


async def shipments_insert(document: Dict[str, Any]) -> Dict[str, Any]:
    body = _build_shipment_payload(document)
    #variants = [
    #    {"method": "POST", "url": MONGO_SHIPMENTS_URL, "json": body},
    #    {"method": "POST", "url": f"{MONGO_SHIPMENTS_URL.rstrip('/')}/insertOne", "json": {"document": body}},
    #    {"method": "POST", "url": f"{MONGO_SHIPMENTS_URL.rstrip('/')}/documents", "json": body},
    #]
    variants = [{"method": "POST", "url": MONGO_SHIPMENTS_URL, "json": body},]
    r = await _try_http_variants(variants)
    try:
        data = r.json()
    except json.JSONDecodeError:
        data = r.text
    logger.info("Shipments insert response: %s", data)
    return data if isinstance(data, dict) else {"raw": data}

# ---- Points REST helpers ----
async def points_find(lat: Any, lon: Any, pointID: Any, belongs_to: Any | None = None) -> Optional[Dict[str, Any]]:
    params = {"lat": lat, "lon": lon, "pointID": pointID}
    if belongs_to is not None:
        params["belongsTo"] = belongs_to

    '''variants = [
        {"method": "GET", "url": MONGO_POINTS_URL, "params": params},
        {"method": "GET", "url": MONGO_POINTS_URL, "params": {"filter": json.dumps(params)}},
    ]
    try:
        r = await _try_http_variants(variants)
        try:
            data = r.json()
        except json.JSONDecodeError:
            data = r.text
        logger.info("Points filtered GET response: %s", data)
        docs = _normalize_list_response(data)
        for d in docs:
            if not isinstance(d, dict):
                continue
            lat_ok = str(d.get("lat")) == str(lat)
            lon_val = d.get("lon") if d.get("lon") is not None else d.get("long")
            lon_ok = str(lon_val) == str(lon)
            point_ok = str(d.get("pointID")) == str(pointID)
            belongs_ok = True if belongs_to is None else str(d.get("belongsTo")) == str(belongs_to)
            if lat_ok and lon_ok and point_ok and belongs_ok:
                return d
    except Exception:
        logger.exception("Points filtered GET failed; attempting full GET")'''

    r_all = await _try_http_variants([{"method": "GET", "url": MONGO_POINTS_URL}])
    try:
        data_all = r_all.json()
    except json.JSONDecodeError:
        data_all = r_all.text
    logger.info("Points Full List Response: %s", data_all)
    docs_all = _normalize_list_response(data_all)
    for d in docs_all:
        if not isinstance(d, dict):
            continue
        lat_ok = str(d.get("lat")) == str(lat)
        lon_val = d.get("lon") if d.get("lon") is not None else d.get("long")
        lon_ok = str(lon_val) == str(lon)
        point_ok = str(d.get("pointID")) == str(pointID)
        belongs_ok = True if belongs_to is None else str(d.get("belongsTo")) == str(belongs_to)
        if lat_ok and lon_ok and point_ok and belongs_ok:
            return d
    return None


def _build_point_payload(document: Dict[str, Any]) -> Dict[str, Any]:
    """Payload for /points: { lat, lon, belongsTo } — map from internal doc."""
    lat = document.get("lat")
    lon = document.get("lon", document.get("long"))
    payload: Dict[str, Any] = {}
    if lat is not None:
        payload["lat"] = lat
    if lon is not None:
        payload["lon"] = lon
    if document.get("belongsTo") is not None:
        payload["pointID"] = document["belongsTo"]
        payload["belongsTo"] = None
    payload["alt"] = 0
    return payload


async def points_insert(document: Dict[str, Any]) -> Dict[str, Any]:
    body = _build_point_payload(document)
    variants = [
        {"method": "POST", "url": MONGO_POINTS_URL, "json": body},
        #{"method": "POST", "url": f"{MONGO_POINTS_URL.rstrip('/')}/insertOne", "json": {"document": body}},
        #{"method": "POST", "url": f"{MONGO_POINTS_URL.rstrip('/')}/documents", "json": body},
    ]
    r = await _try_http_variants(variants)
    try:
        data = r.json()
    except json.JSONDecodeError:
        data = r.text
    logger.info("Points insert response: %s", data)
    return data if isinstance(data, dict) else {"raw": data}

# ---------------------- Transform helpers ----------------------

def _get_case_insensitive(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    if key in d:
        return d[key]
    lk = key.lower()
    for k, v in d.items():
        if k.lower() == lk:
            return v
    return default


async def process_barcode(barcode: str, tt_token: str, crn_token: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    logger.info("Processing barcode=%s", barcode)
    tt_data = await tt_select_by_barcode(tt_token, barcode)
    if isinstance(tt_data, list) and tt_data:
        tt_data = tt_data[0]
    if not isinstance(tt_data, dict):
        raise UpstreamError(f"Unexpected TT payload for {barcode}: {tt_data!r}")

    postalname = _get_case_insensitive(tt_data, "Postalname")
    note = _get_case_insensitive(tt_data, "Note")
    logger.info("Extracted TT fields for barcode=%s origin=%s note=%s", barcode, postalname, note)

    destination = None
    lat = None
    lon = None
    crn_raw: Dict[str, Any] | None = None

    if note:
        crn_raw = await crn_get_address(crn_token, str(note))
        crn_obj = None
        if isinstance(crn_raw, list) and crn_raw:
            crn_obj = crn_raw[0]
        elif isinstance(crn_raw, dict):
            crn_obj = crn_raw
        if crn_obj:
            destination = _get_case_insensitive(crn_obj, "Town")
            lat = _get_case_insensitive(crn_obj, "Latitude")
            lon = _get_case_insensitive(crn_obj, "Longitude")
            logger.info("Extracted CRN fields for barcode=%s destination=%s lat=%s lon=%s", barcode, destination, lat, lon)

    shipment_doc = {
        "shipmentExternalID": barcode,
        "destination": (destination if destination else ""),
        # Defaults and TT-derived fields added below via _build_shipment_payload
        # We'll pass through pickUpDate / deliveryDate here if we have EventDate
        "pickUpDate": _get_case_insensitive(tt_data, "EventDate"),
        "deliveryDate": _get_case_insensitive(tt_data, "EventDate"),
        # Optional: if you prefer to carry origin from TT, uncomment the next line;
        # the payload builder already defaults to "N/A" if not set.
        # "origin": postalname,
        #"_meta": {
        #    "tt_raw": tt_data,
        #    "crn_raw": crn_raw,
        #    "processedAt": datetime.now(timezone.utc).isoformat(),
        #},
    }

    point_doc = None
    if lat is not None and lon is not None:
        point_doc = {
            "shipmentExternalID": barcode,
            "belongsTo": destination,
            "pointID": barcode,
            "lat": lat,
            "long": lon,
            #"_meta": {"processedAt": datetime.now(timezone.utc).isoformat()},
        }

    logger.debug("Prepared documents for barcode=%s shipment_doc=%s point_doc=%s", barcode, shipment_doc, point_doc)
    return shipment_doc, point_doc

# ---------------------- Persistence & existence check ----------------------
async def shipment_exists(barcode: str) -> bool:
    doc = await shipments_find_by_external_id(barcode)
    exists = doc is not None
    logger.info("shipment_exists(%s) -> %s", barcode, exists)
    return exists


async def upsert_documents(
    shipment_doc: Optional[Dict[str, Any]],
    point_doc: Optional[Dict[str, Any]],
) -> None:

    # Insert point if not present (dedupe by lat+lon+pointID+belongsTo)
    if point_doc:
        lat = point_doc.get("lat")
        lon = point_doc.get("long", point_doc.get("lon"))
        pointID = point_doc.get("pointID")
        if lat is None or lon is None:
            logger.warning("Point doc missing lat/lon; skipping insert: %s", point_doc)
            return
        existing_point = await points_find(lat, lon, pointID, point_doc.get("belongsTo"))
        if existing_point:
            logger.info("Point already exists for lat=%s lon=%s pointID=%s belongsTo=%s — skipping insert", lat, lon, pointID,  point_doc.get("belongsTo"))
        else:
            logger.info("Inserting point for lat=%s lon=%s belongsTo=%s", lat, lon, point_doc.get("belongsTo"))
            if "createdAt" not in point_doc:
                point_doc["createdAt"] = datetime.now(timezone.utc).isoformat()
            await points_insert(point_doc)
    # Insert shipment (destination + shipmentExternalID only)
    if shipment_doc:
        logger.info(
            "Inserting into shipments for shipmentExternalID=%s",
            shipment_doc.get("shipmentExternalID"),
        )
        if "createdAt" not in shipment_doc:
            shipment_doc["createdAt"] = datetime.now(timezone.utc).isoformat()
        await shipments_insert(shipment_doc)

# ---------------------- Worker ----------------------
async def scan_and_process_once() -> Dict[str, Any]:
    logger.info("Starting scan-and-process cycle")
    tt_token = await fetch_oauth_token(TT_OAUTH_URL, BASIC_USER, BASIC_PASS, USER_TOKEN)
    crn_token = await fetch_oauth_token(CRN_OAUTH_URL, BASIC_USER, BASIC_PASS, USER_TOKEN)

    processed = 0
    skipped_existing = 0
    errors: Dict[str, str] = {}

    try:
        with open(SHIPMENTS_FILE, 'r', encoding='utf-8') as f:
            lines = [ln.strip() for ln in f if ln.strip()]
    except FileNotFoundError:
        logger.warning("shipments.txt not found; nothing to process")
        return {"processed": 0, "skipped_existing": 0, "errors": {"_global": "shipments.txt not found"}}

    for barcode in lines:
        try:
            if await shipment_exists(barcode):
                skipped_existing += 1
                logger.info("Skipping barcode=%s (shipment already exists)", barcode)
                continue

            shipment_doc, point_doc = await process_barcode(barcode, tt_token, crn_token)
            await upsert_documents(shipment_doc, point_doc)
            processed += 1
        except Exception as e:  # noqa: BLE001
            logger.exception("Error processing barcode %s", barcode)
            errors[barcode] = str(e)

    logger.info("Cycle complete: processed=%s skipped_existing=%s errors=%s", processed, skipped_existing, errors)
    return {"processed": processed, "skipped_existing": skipped_existing, "errors": errors}


async def run_forever():
    logger.info("Background ingestor started; interval=%ss", POLL_SECONDS)
    try:
        while True:
            try:
                stats = await scan_and_process_once()
                logger.info("Run complete: %s", stats)
            except Exception:
                logger.exception("Top-level failure in scan cycle")
            await asyncio.sleep(POLL_SECONDS)
    finally:
        await client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        logger.info("Shutting down on KeyboardInterrupt")
