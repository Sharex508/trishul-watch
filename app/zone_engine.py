import logging
from typing import List, Dict, Optional, Tuple
import json

try:
    import numpy as np
except Exception:
    np = None

from .coin_price_monitor import get_database_connection
from .db_schema import is_pg
from .coin_price_monitor import _q

TF_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "60m": 3_600_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


def timeframe_to_ms(tf: str) -> int:
    """Return milliseconds per bar for a timeframe string."""
    return TF_MS.get(tf, 0)

def classify_candle(open_p: float, high: float, low: float, close: float) -> dict:
    """Return body_pct and is_boring per 50% rule."""
    rng = high - low
    body = abs(close - open_p)
    body_pct = body / rng if rng else 0.0
    return {"body_pct": body_pct, "is_boring": body_pct <= 0.5}


def fetch_candles(symbol: str, timeframe: str, limit: int = 200) -> List[dict]:
    conn = cur = None
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        sql = f"SELECT ts, open, high, low, close, volume FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts ASC LIMIT {int(limit)}"
        cur.execute(sql.replace("?", "%s") if pg else sql, (symbol, timeframe))
        rows = cur.fetchall()
        out = []
        for r in rows:
            out.append({"ts": int(r[0]), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4]), "volume": float(r[5])})
        return out
    except Exception as e:
        logging.warning(f"zone fetch_candles failed for {symbol}: {e}")
        return []
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


def fetch_latest_price(symbol: str) -> Optional[float]:
    conn = cur = None
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        sql = "SELECT latest_price FROM coin_monitor WHERE symbol = ?"
        cur.execute(sql.replace("?", "%s") if pg else sql, (symbol,))
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None
    except Exception:
        return None
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


def fetch_zones_for_symbol(symbol: str) -> List[dict]:
    """Return all zones for symbol across timeframes."""
    conn = cur = None
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        sql = "SELECT id, timeframe, zone_type, formation, proximal, distal FROM zones WHERE symbol = ?"
        cur.execute(_q(sql, pg), (symbol,))
        rows = cur.fetchall()
        cols = ["id","timeframe","zone_type","formation","proximal","distal"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception:
        return []
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


def fetch_latest_atr(symbol: str, timeframe: str = "1m") -> Optional[float]:
    """Return latest ATR from features table if available."""
    conn = cur = None
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        sql = "SELECT atr FROM features WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 1"
        cur.execute(_q(sql, pg), (symbol, timeframe))
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None
    except Exception:
        return None
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


def select_htf(base_tf: str) -> str:
    """Choose a higher timeframe for curve/trend context."""
    mapping = {
        "1m": "15m",
        "5m": "1h",
        "15m": "4h",
        "30m": "4h",
        "60m": "1d",
    }
    return mapping.get(base_tf, base_tf)


def compute_curve_location_from_zones(symbol: str, base_tf: str = "1m") -> str:
    """
    Curve from HTF zones: find nearest demand below and supply above current price.
    """
    price = fetch_latest_price(symbol)
    if price is None:
        return compute_curve_location(symbol, timeframe=base_tf)
    htf = select_htf(base_tf)
    zones = [z for z in fetch_zones_for_symbol(symbol) if z["timeframe"] == htf]
    if not zones:
        return compute_curve_location(symbol, timeframe=base_tf)
    demands = [z for z in zones if z["zone_type"] == "demand"]
    supplies = [z for z in zones if z["zone_type"] == "supply"]
    below = max([z["proximal"] for z in demands if z["proximal"] <= price], default=None)
    above = min([z["proximal"] for z in supplies if z["proximal"] >= price], default=None)
    if below is None or above is None:
        return "equilibrium"
    span = above - below
    if span <= 0:
        return "equilibrium"
    pct = (price - below) / span
    if pct <= 0.2:
        return "very_low"
    if pct <= 0.4:
        return "low"
    if pct <= 0.6:
        return "equilibrium"
    if pct <= 0.8:
        return "high"
    return "very_high"


def compute_trend_from_zones(symbol: str, base_tf: str = "1m") -> str:
    """
    Trend from MTFA zone violations: up if price has violated 2 supply zones, down if 2 demand zones, else sideways.
    """
    price = fetch_latest_price(symbol)
    if price is None:
        return compute_trend(symbol, timeframe=base_tf)
    itf = select_htf(base_tf)
    zones = [z for z in fetch_zones_for_symbol(symbol) if z["timeframe"] == itf]
    if not zones:
        return compute_trend(symbol, timeframe=base_tf)
    supply_breaches = 0
    demand_breaches = 0
    for z in zones:
        if z["zone_type"] == "supply" and price > z["proximal"]:
            supply_breaches += 1
        if z["zone_type"] == "demand" and price < z["proximal"]:
            demand_breaches += 1
    if supply_breaches >= 2:
        return "up"
    if demand_breaches >= 2:
        return "down"
    return "sideways"


def get_existing_zones(symbol: str, timeframe: str) -> List[dict]:
    conn = cur = None
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        sql = "SELECT id, zone_type, proximal, distal FROM zones WHERE symbol = ? AND timeframe = ?"
        cur.execute(_q(sql, pg), (symbol, timeframe))
        rows = cur.fetchall()
        out = []
        for r in rows:
            out.append({"id": int(r[0]), "zone_type": r[1], "proximal": float(r[2]), "distal": float(r[3])})
        return out
    except Exception:
        return []
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


def compute_curve_location(symbol: str, timeframe: str = "1m") -> str:
    """
    Rough curve location using recent min/max closes (quintiles):
    Very Low, Low, Equilibrium, High, Very High.
    """
    candles = fetch_candles(symbol, timeframe, limit=200)
    if not candles:
        return "unknown"
    closes = [c["close"] for c in candles]
    min_c, max_c = min(closes), max(closes)
    last = closes[-1]
    if max_c - min_c == 0:
        return "equilibrium"
    pct = (last - min_c) / (max_c - min_c)
    if pct <= 0.2:
        return "very_low"
    if pct <= 0.4:
        return "low"
    if pct <= 0.6:
        return "equilibrium"
    if pct <= 0.8:
        return "high"
    return "very_high"


def compute_trend(symbol: str, timeframe: str = "1m") -> str:
    """
    Simple trend heuristic: compare latest close to close N bars ago and slope of closes.
    """
    candles = fetch_candles(symbol, timeframe, limit=80)
    if len(candles) < 10:
        return "sideways"
    closes = [c["close"] for c in candles]
    last = closes[-1]
    prev = closes[-10]
    delta = (last - prev) / prev if prev else 0.0
    if delta > 0.01:
        return "up"
    if delta < -0.01:
        return "down"
    return "sideways"


def detect_zones_for_symbol(symbol: str, timeframe: str = "1m", max_bars: int = 300) -> Tuple[List[dict], dict]:
    """
    Detect DBR/RBR/RBD/DBD based on boring base between exciting legs.
    Returns zones and skip_reason counters for observability.
    """
    candles = fetch_candles(symbol, timeframe, limit=max_bars)
    skip_reasons = {
        "not_enough_candles": 0,
        "no_boring_base": 0,
        "missing_exciting_leg": 0,
        "unknown_direction": 0,
        "weak_leg_out": 0,
        "base_len_reject": 0,
    }
    if len(candles) < 20:
        skip_reasons["not_enough_candles"] = 1
        return [], skip_reasons
    zones: List[dict] = []
    tf_ms = timeframe_to_ms(timeframe)
    # classify candles
    for c in candles:
        cls = classify_candle(c["open"], c["high"], c["low"], c["close"])
        c.update(cls)
    last_price = candles[-1]["close"]

    # iterate and find bases: consecutive boring candles 1-7 long, with exciting candles before/after
    i = 1
    n = len(candles)
    while i < n - 2:
        if not candles[i]["is_boring"]:
            skip_reasons["no_boring_base"] += 1
            i += 1
            continue
        base_start = i
        base_end = i
        while base_end + 1 < n and candles[base_end + 1]["is_boring"] and (base_end - base_start) < 7:
            base_end += 1
        # need exciting candle before and after
        if base_start == 0 or base_end >= n - 1:
            i = base_end + 1
            continue
        leg_in = candles[base_start - 1]
        leg_out = candles[base_end + 1]
        if leg_in["is_boring"] or leg_out["is_boring"]:
            skip_reasons["missing_exciting_leg"] += 1
            i = base_end + 1
            continue
        # direction check
        leg_in_dir_down = leg_in["close"] < leg_in["open"]
        leg_out_dir_up = leg_out["close"] > leg_out["open"]
        formation = None
        zone_type = None
        if leg_in_dir_down and leg_out_dir_up:
            formation = "DBR"
            zone_type = "demand"
        elif (not leg_in_dir_down) and leg_out_dir_up:
            formation = "RBR"
            zone_type = "demand"
        elif (not leg_in_dir_down) and (not leg_out_dir_up):
            formation = "RBD"
            zone_type = "supply"
        elif leg_in_dir_down and (not leg_out_dir_up):
            formation = "DBD"
            zone_type = "supply"
        else:
            formation = "UNK"
            zone_type = "unknown"
        # skip unknown
        if formation == "UNK":
            skip_reasons["unknown_direction"] += 1
            i = base_end + 1
            continue
        # proximal/distal per simplified rules
        base_slice = candles[base_start:base_end + 1]
        highs = [c["close"] for c in base_slice]
        lows = [c["close"] for c in base_slice]
        if zone_type == "demand":
            proximal = max(highs)
            distal = min([c["low"] for c in base_slice + [leg_in]])
        else:
            proximal = min(lows)
            distal = max([c["high"] for c in base_slice + [leg_in]])
        # strength filter: leg_out range vs zone height
        zone_height = abs(proximal - distal) if proximal and distal else 0.0
        leg_out_range = abs(leg_out["close"] - leg_out["open"])
        if zone_height == 0 or leg_out_range < zone_height * 1.0:  # slightly relaxed vs 1.2x
            skip_reasons["weak_leg_out"] += 1
            i = base_end + 1
            continue
        # base candle count filter
        base_len = (base_end - base_start) + 1
        if base_len < 1 or base_len > 8:
            skip_reasons["base_len_reject"] += 1
            i = base_end + 1
            continue
        leg_ratio = (leg_out_range or 0) / (zone_height or 1e-9)
        age_bars = 0.0
        if tf_ms and candles[-1]["ts"] and leg_out.get("ts"):
            try:
                age_bars = max(0.0, (candles[-1]["ts"] - leg_out["ts"]) / tf_ms)
            except Exception:
                age_bars = 0.0
        zone = {
            "symbol": symbol,
            "timeframe": timeframe,
            "zone_type": zone_type,
            "formation": formation,
            "proximal": proximal,
            "distal": distal,
            "base_start_ts": candles[base_start]["ts"],
            "base_end_ts": candles[base_end]["ts"],
            "leg_in_ts": leg_in["ts"],
            "leg_out_ts": leg_out["ts"],
            "base_len": base_len,
            "leg_out_range": leg_out_range,
            "zone_height": zone_height,
            "leg_ratio": leg_ratio,
            "age_bars": age_bars,
            "tests": 1 if distal <= last_price <= proximal else 0,
        }
        zones.append(zone)
        i = base_end + 1
    if not zones and skip_reasons["no_boring_base"] == 0:
        skip_reasons["no_boring_base"] = 1
    return zones, skip_reasons


def filter_overlapping_zones(zones: List[dict]) -> Tuple[List[dict], int]:
    """
    Drop overlapping zones, keeping the strongest (highest leg_ratio then shortest base).
    """
    kept: List[dict] = []
    skipped = 0
    sorted_zones = sorted(
        zones,
        key=lambda z: (z.get("leg_ratio", 0.0), -(z.get("base_len") or 0)),
        reverse=True,
    )
    for z in sorted_zones:
        z_low = min(z["proximal"], z["distal"])
        z_high = max(z["proximal"], z["distal"])
        conflict = False
        for k in kept:
            k_low = min(k["proximal"], k["distal"])
            k_high = max(k["proximal"], k["distal"])
            if max(z_low, k_low) <= min(z_high, k_high):
                conflict = True
                break
        if conflict:
            skipped += 1
            continue
        kept.append(z)
    return kept, skipped


def score_zone(zone: dict, opposing_dist: Optional[float] = None) -> Tuple[float, float, str, str, float, float]:
    """Basic/advanced scores and labels (PDF-inspired boosters with freshness decay and alignment)."""
    basic = 0.0
    tests = zone.get("tests", 0) or 0
    age_bars = zone.get("age_bars", 0.0) or 0.0

    # Freshness: untouched + recency, decay on age/tests
    freshness_score = 3.0 if tests == 0 else (2.0 if tests == 1 else 1.0)
    if age_bars > 300:
        freshness_score -= 2.0
    elif age_bars > 150:
        freshness_score -= 1.0
    elif age_bars > 60:
        freshness_score -= 0.5
    freshness_score = max(freshness_score, 0.0)
    basic += freshness_score

    # Strength of move: leg_out vs zone height
    leg_ratio = (zone.get("leg_out_range") or 0) / (zone.get("zone_height") or 1e-9)
    if leg_ratio >= 3:
        basic += 3
    elif leg_ratio >= 2:
        basic += 2
    elif leg_ratio >= 1.2:
        basic += 1

    # Time at zone (base length)
    base_len = zone.get("base_len", 0)
    if 1 <= base_len <= 3:
        basic += 2
    elif base_len <= 6:
        basic += 1
    elif base_len <= 8:
        basic += 0.5

    # Reward:Risk proxy using distance to opposing zone (HTF if available)
    rr_est = None
    if opposing_dist is not None and zone.get("zone_height"):
        rr_est = opposing_dist / (zone["zone_height"] or 1e-9)
        if rr_est >= 4:
            basic += 3.5
        elif rr_est >= 3:
            basic += 3
        elif rr_est >= 2:
            basic += 2
        elif rr_est >= 1.2:
            basic += 1
    else:
        # small boost if we cannot compute RR yet
        basic += 0.5

    # Advanced probability (curve/trend alignment, confluence)
    adv = 0.0
    loc = zone.get("curve_location", "equilibrium")
    if loc in ("very_low", "very_high"):
        adv += 2
    elif loc in ("low", "high"):
        adv += 1

    trend = zone.get("trend", "sideways")
    if (trend == "up" and zone.get("zone_type") == "demand") or (trend == "down" and zone.get("zone_type") == "supply"):
        adv += 1.5
    elif trend == "sideways":
        adv += 0.25

    if zone.get("confluence"):
        adv += 1
    if zone.get("lotl"):
        adv += 0.5
    if zone.get("trap"):
        adv += 0.5

    quality_label = "low"
    if basic >= 11:
        quality_label = "high"
    elif basic >= 8:
        quality_label = "medium"
    probability_label = "high" if adv >= 4 else ("medium" if adv >= 2.5 else "low")
    return basic, adv, quality_label, probability_label, rr_est, freshness_score


def persist_zones(zones: List[dict], opposing_external: Optional[List[dict]] = None):
    if not zones:
        return 0
    conn = cur = None
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        ins = """
            INSERT INTO zones(symbol, timeframe, zone_type, formation, proximal, distal, base_start_ts, base_end_ts, leg_in_ts, leg_out_ts, quality_basic, quality_adv, quality_label, probability_label, rr_est, curve_location, trend, freshness, tests, opposing_dist, opposing_zone_id, confluence, lotl, trap)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        ins = ins.replace("?", "%s") if pg else ins
        # helper to find nearest opposing zone inside the detected set
        def nearest_opposing(z):
            opp_type = "demand" if z["zone_type"] == "supply" else "supply"
            best = None
            candidates = []
            candidates.extend(zones)
            if opposing_external:
                candidates.extend(opposing_external)
            for o in candidates:
                if o is z or o.get("zone_type") != opp_type:
                    continue
                dist = abs(z["proximal"] - o["proximal"])
                if best is None or dist < best[0]:
                    best = (dist, o.get("id"))
            return best

        for z in zones:
            z.setdefault("tests", 0)
            z.setdefault("freshness", 0)
            z.setdefault("confluence", 0)
            z.setdefault("lotl", 0)
            z.setdefault("trap", 0)
            z.setdefault("curve_location", "unknown")
            z.setdefault("trend", "sideways")
            opp = nearest_opposing(z)
            opp_dist = opp[0] if opp else None
            opp_id = opp[1] if opp else None
            b, a, qlabel, plabel, rr_est, fresh = score_zone(z, opposing_dist=opp_dist)
            cur.execute(ins, (z["symbol"], z["timeframe"], z["zone_type"], z["formation"], z["proximal"], z["distal"],
                              z["base_start_ts"], z["base_end_ts"], z["leg_in_ts"], z["leg_out_ts"], b, a, qlabel, plabel,
                              rr_est, z.get("curve_location"), z.get("trend"), fresh, z.get("tests", 0), opp_dist, opp_id,
                              z.get("confluence", 0), z.get("lotl", 0), z.get("trap", 0)))
        conn.commit()
        return len(zones)
    except Exception as e:
        logging.error(f"Persist zones failed: {e}")
        return 0
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


def run_zone_detection(symbols: List[str], timeframe: str = "1m"):
    total = 0
    for sym in symbols:
        try:
            # compute context from HTF/ITF zones
            curve_loc = compute_curve_location_from_zones(sym, base_tf=timeframe)
            trend = compute_trend_from_zones(sym, base_tf=timeframe)
            # clear existing zones for symbol/timeframe to avoid duplicates
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            cur.execute(_q("DELETE FROM zones WHERE symbol = ? AND timeframe = ?", pg), (sym, timeframe))
            conn.commit()
            try:
                cur.close(); conn.close()
            except Exception:
                pass
            existing_all = fetch_zones_for_symbol(sym)
            zs, skip_reasons = detect_zones_for_symbol(sym, timeframe=timeframe)
            zs, overlap_skips = filter_overlapping_zones(zs)
            # attach context for scoring
            for z in zs:
                z["curve_location"] = curve_loc
                z["trend"] = trend
                # confluence: overlap with zones on other tfs
                for o in existing_all:
                    if o.get("timeframe") == timeframe:
                        continue
                    o_low = min(o["proximal"], o["distal"])
                    o_high = max(o["proximal"], o["distal"])
                    z_low = min(z["proximal"], z["distal"])
                    z_high = max(z["proximal"], z["distal"])
                    if max(o_low, z_low) <= min(o_high, z_high):
                        z["confluence"] = 1
                # LOTL: another zone same tf/type within height
                for o in zs:
                    if o is z:
                        continue
                    if o["zone_type"] != z["zone_type"]:
                        continue
                    z_low = min(z["proximal"], z["distal"])
                    z_high = max(z["proximal"], z["distal"])
                    o_low = min(o["proximal"], o["distal"])
                    o_high = max(o["proximal"], o["distal"])
                    if max(z_low, o_low) <= min(z_high, o_high) or abs(z["proximal"] - o["proximal"]) <= z.get("zone_height", 0) * 1.0:
                        z["lotl"] = 1
            inserted = persist_zones(zs, opposing_external=existing_all)
            total += inserted
            logging.info(
                f"Zones {sym} {timeframe}: found {len(zs)}, inserted {inserted}, overlap_skipped {overlap_skips}, "
                f"skips {skip_reasons}, context curve={curve_loc} trend={trend}"
            )
        except Exception as e:
            logging.warning(f"Zone detection failed for {sym}: {e}")
    return total


def get_zone_by_id(zone_id: int) -> Optional[dict]:
    conn = cur = None
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        sql = "SELECT id, symbol, timeframe, zone_type, formation, proximal, distal, quality_basic, quality_adv, quality_label, probability_label, curve_location, trend FROM zones WHERE id = ?"
        cur.execute(sql.replace("?", "%s") if pg else sql, (zone_id,))
        row = cur.fetchone()
        if not row:
            return None
        cols = ["id","symbol","timeframe","zone_type","formation","proximal","distal","quality_basic","quality_adv","quality_label","probability_label","curve_location","trend"]
        return dict(zip(cols, row))
    except Exception:
        return None
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


def plan_entry_for_zone(zone: dict, balance: float = 1000.0, risk_perc: float = 1.0, rr_target: float = 2.0, atr_mult: float = 1.5, buffer_pct: float = 0.001) -> dict:
    """
    Suggest entry/stop/tp and entry type based on zone quality/probability.
    """
    if not zone:
        raise ValueError("Zone data required")
    zone_type = zone.get("zone_type")
    proximal = float(zone.get("proximal"))
    distal = float(zone.get("distal"))

    # Choose entry type from quality/probability
    quality = zone.get("quality_label", "low")
    probability = zone.get("probability_label", "low")
    curve = zone.get("curve_location", "unknown")
    trend = zone.get("trend", "sideways")
    if quality == "high" and probability in ("high", "medium") and ((trend == "up" and zone_type == "demand") or (trend == "down" and zone_type == "supply")):
        entry_type = "type1"
    elif quality in ("medium", "high") and probability in ("high", "medium"):
        entry_type = "type2"
    else:
        entry_type = "type3"

    # Entry/Stop with ATR/distal buffer
    entry_price = proximal
    atr_val = fetch_latest_atr(zone.get("symbol"), timeframe=zone.get("timeframe", "1m"))
    atr_buffer = (atr_val or 0) * atr_mult if atr_val is not None else 0.0
    pct_buffer = proximal * buffer_pct
    buffer = max(atr_buffer, pct_buffer)
    if zone_type == "demand":
        stop_price = distal - buffer
        tp_price = entry_price + abs(entry_price - stop_price) * rr_target
    else:
        stop_price = distal + buffer
        tp_price = entry_price - abs(stop_price - entry_price) * rr_target

    stop_dist = abs(entry_price - stop_price)
    risk_amount = balance * (risk_perc / 100.0)
    qty = risk_amount / stop_dist if stop_dist > 0 else 0.0

    return {
        "zone_id": zone.get("id"),
        "symbol": zone.get("symbol"),
        "timeframe": zone.get("timeframe"),
        "zone_type": zone_type,
        "entry_type": entry_type,
        "entry_price": entry_price,
        "stop_price": stop_price,
        "take_profit_price": tp_price,
        "risk_amount": risk_amount,
        "position_size": qty,
        "breakeven_at": entry_price + stop_dist if zone_type == "demand" else entry_price - stop_dist,
        "risk_perc": risk_perc,
        "rr_target": rr_target,
        "atr_used": atr_val,
        "buffer_used": buffer,
        "curve_location": curve,
        "trend": trend,
        "r_value": stop_dist,
        "balance": balance,
    }


def persist_entry_plan(plan: dict) -> Optional[int]:
    """Store entry plan into entry_plans table."""
    if not plan:
        return None
    conn = cur = None
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        cols = [
            "zone_id","symbol","timeframe","entry_type","entry_price","stop_price",
            "take_profit_price","rr_target","risk_perc","balance","position_size",
            "risk_amount","atr_used","buffer_used","status"
        ]
        placeholders = ', '.join(['?'] * len(cols))
        ins = f"INSERT INTO entry_plans({', '.join(cols)}) VALUES ({placeholders})"
        if pg:
            ins += " RETURNING id"
        cur.execute(ins.replace("?", "%s") if pg else ins, (
            plan.get("zone_id"),
            plan.get("symbol"),
            plan.get("timeframe"),
            plan.get("entry_type"),
            plan.get("entry_price"),
            plan.get("stop_price"),
            plan.get("take_profit_price"),
            plan.get("rr_target"),
            plan.get("risk_perc"),
            plan.get("balance"),
            plan.get("position_size"),
            plan.get("risk_amount"),
            plan.get("atr_used"),
            plan.get("buffer_used"),
            plan.get("status", "planned"),
        ))
        if pg:
            rid_row = cur.fetchone()
            rid = rid_row[0] if rid_row else None
        else:
            try:
                cur.execute("SELECT last_insert_rowid()")
                rid = cur.fetchone()[0]
            except Exception:
                rid = None
        conn.commit()
        return rid
    except Exception as e:
        logging.warning(f"Persist entry plan failed: {e}")
        return None
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass
