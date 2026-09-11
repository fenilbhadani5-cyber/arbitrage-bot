import json
from collections import Counter, defaultdict, deque

def load(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
                r["_line"] = i
                rows.append(r)
            except Exception as e:
                print("parse fail", path, i, e)
    return rows

live = load(r"D:\Arbitrage\live_trades.jsonl")
missed = load(r"D:\Arbitrage\missed_trades.jsonl")

print("==== LIVE TRADES ====")
opens = [r for r in live if r.get("trade_type") == "Open"]
closes = [r for r in live if r.get("trade_type") == "Close"]
print("total", len(live), "opens", len(opens), "closes", len(closes))
print("records with latency field", sum(1 for r in live if r.get("latency")))
print("open keys sample", sorted((opens[-1] if opens else {}).keys()))

def dump_row(r):
    lat = r.get("latency") or {}
    ba = r.get("buy_book_ask") or 0
    sb = r.get("sell_book_bid") or 0
    bf = r.get("buy_fill_price") or 0
    sf = r.get("sell_fill_price") or 0
    buy_slip = ((bf - ba) / ba * 100) if ba else None
    sell_slip = ((sb - sf) / sb * 100) if sb else None
    quoted = ((sb - ba) / ba * 100) if ba else None
    captured = ((sf - bf) / bf * 100) if bf else None
    print(
        f"  id={r.get('id')} ts={r.get('timestamp')} coin={r.get('coin')} type={r.get('trade_type')} "
        f"buy={r.get('exchange_buy')} sell={r.get('exchange_sell')}"
    )
    print(
        f"    book_ask={ba} book_bid={sb} fill_buy={bf} fill_sell={sf} "
        f"quoted={quoted} captured={captured} buy_slip={buy_slip} sell_slip={sell_slip}"
    )
    print(
        f"    spread_before={r.get('spread_before')} entry={r.get('entry_spread')} "
        f"exit={r.get('exit_spread')} hold={r.get('hold_duration_secs')} pnl_net={r.get('pnl_net')} "
        f"buy_qty={r.get('buy_filled_qty')} sell_qty={r.get('sell_filled_qty')} "
        f"buy_quote={r.get('buy_quote_value')} sell_quote={r.get('sell_quote_value')}"
    )
    if lat:
        print(
            f"    LAT det->send={lat.get('detection_to_send_ms')} send->ack={lat.get('send_to_ack_ms')} "
            f"ack->fill={lat.get('ack_to_fill_ms')} book->send={lat.get('book_to_send_ms')} "
            f"total={lat.get('total_pipeline_ms')} buy_rtt={lat.get('buy_leg_rtt_ms')} "
            f"sell_rtt={lat.get('sell_leg_rtt_ms')} rev={lat.get('reversal_rtt_ms')} diag={lat.get('latency_diagnosis')}"
        )
        print(
            f"    quotes det_ask={lat.get('detected_buy_ask')} det_bid={lat.get('detected_sell_bid')} "
            f"det_spread={lat.get('detected_spread_pct')} pre_ask={lat.get('preflight_buy_ask')} "
            f"pre_bid={lat.get('preflight_sell_bid')} pre_spread={lat.get('preflight_spread_pct')}"
        )
        print(
            f"    stamps book={lat.get('book_update_ms')} det={lat.get('opportunity_detected_ms')} "
            f"pre={lat.get('pre_flight_check_ms')} send={lat.get('order_send_ms')} "
            f"ack={lat.get('exchange_ack_ms')} fill={lat.get('ws_fill_ms')}"
        )
    else:
        print("    NO LATENCY FIELD")

print("\n-- OPENS --")
for r in opens:
    dump_row(r)

print("\n-- CLOSES --")
for r in closes:
    dump_row(r)

print("\n==== PAIRED ROUNDTRIPS ====")
pending = defaultdict(deque)
for r in live:
    coin = r.get("coin")
    if r.get("trade_type") == "Open":
        pending[coin].append(r)
    elif r.get("trade_type") == "Close":
        if pending[coin]:
            o = pending[coin].popleft()
            ba = o.get("buy_book_ask") or 0
            sb = o.get("sell_book_bid") or 0
            bf = o.get("buy_fill_price") or 0
            sf = o.get("sell_fill_price") or 0
            quoted = ((sb - ba) / ba * 100) if ba and sb else float("nan")
            captured = ((sf - bf) / bf * 100) if bf and sf else float("nan")
            buy_slip = ((bf - ba) / ba * 100) if ba else float("nan")
            sell_slip = ((sb - sf) / sb * 100) if sb else float("nan")
            print(
                f"  {coin:8} hold={r.get('hold_duration_secs')}s quoted={quoted:.3f}% captured={captured:.3f}% "
                f"buy_slip={buy_slip:.3f}% sell_slip={sell_slip:.3f}% "
                f"entry={r.get('entry_spread')} exit={r.get('exit_spread')} "
                f"pnl={r.get('pnl_net'):.4f} fees={r.get('total_fee'):.4f} "
                f"open={o.get('timestamp')} close={r.get('timestamp')}"
            )
        else:
            print("  UNMATCHED CLOSE", coin, r.get("id"))
for c, q in pending.items():
    if q:
        print("  UNMATCHED OPEN", c, [x.get("id") for x in q])

print("\n==== MISSED TRADES ====")
print("records", len(missed))
print("with latency", sum(1 for r in missed if r.get("latency")))
cats = Counter()
for r in missed:
    reason = r.get("reason") or "unknown"
    cat = reason.split(":")[0].split("(")[0].strip()
    cats[cat] += 1
print("categories:")
for k, v in cats.most_common():
    print(f"  {v:5d}  {k}")

print("\n-- LEG / EXECUTION FAILURES (full) --")
for r in missed:
    reason = r.get("reason") or ""
    cat = reason.split(":")[0].strip()
    if cat in ("LEG_FILL_FAILED", "BOTH_LEGS_FAILED", "ONE_LEG_FAILED", "PRE_FLIGHT_SPREAD_COLLAPSED", "LOW_LIQUIDITY", "INSUFFICIENT_MARGIN_BUY", "INSUFFICIENT_MARGIN_SELL", "INSUFFICIENT_BALANCE", "BELOW_MIN_NOTIONAL", "NEAR_FUNDING"):
        lat = r.get("latency") or {}
        print(f"  {r.get('timestamp')} {r.get('coin'):8} spread={r.get('spread_pct'):.3f} book={r.get('book_spread_pct')} bal B={r.get('binance_balance'):.2f} Y={r.get('bybit_balance'):.2f}")
        print(f"    {reason}")
        if lat:
            print(
                f"    LAT det->send={lat.get('detection_to_send_ms')} send->ack={lat.get('send_to_ack_ms')} "
                f"book->send={lat.get('book_to_send_ms')} total={lat.get('total_pipeline_ms')} "
                f"buy_rtt={lat.get('buy_leg_rtt_ms')} sell_rtt={lat.get('sell_leg_rtt_ms')} "
                f"rev={lat.get('reversal_rtt_ms')} diag={lat.get('latency_diagnosis')}"
            )
            print(
                f"    det_spread={lat.get('detected_spread_pct')} pre_spread={lat.get('preflight_spread_pct')} "
                f"det_ask={lat.get('detected_buy_ask')} pre_ask={lat.get('preflight_buy_ask')}"
            )

print("\n-- latency-bearing missed records (any) --")
n = 0
for r in missed:
    lat = r.get("latency")
    if not lat:
        continue
    n += 1
    print(
        f"  {r.get('timestamp')} {r.get('coin'):8} cat={(r.get('reason') or '').split(':')[0]} "
        f"det->send={lat.get('detection_to_send_ms')} send->ack={lat.get('send_to_ack_ms')} "
        f"book->send={lat.get('book_to_send_ms')} total={lat.get('total_pipeline_ms')} "
        f"buy_rtt={lat.get('buy_leg_rtt_ms')} sell_rtt={lat.get('sell_leg_rtt_ms')} "
        f"rev={lat.get('reversal_rtt_ms')} diag={lat.get('latency_diagnosis')}"
    )
print("latency missed count", n)

print("\n==== MISSED BY HOUR ====")
hours = Counter()
for r in missed:
    ts = r.get("timestamp") or ""
    hours[ts[:13]] += 1
for h, v in sorted(hours.items()):
    print(f"  {h}  {v}")

print("\n==== SESSION LIMIT / PAUSE / HALT TIMELINE (first/last) ====")
for cat in ["SESSION_LIMIT_REACHED", "TRADING_PAUSED", "EMERGENCY_HALT", "LEG_FILL_FAILED", "PRE_FLIGHT_SPREAD_COLLAPSED"]:
    subset = [r for r in missed if (r.get("reason") or "").startswith(cat)]
    if not subset:
        continue
    print(f"  {cat}: {len(subset)} first={subset[0].get('timestamp')} last={subset[-1].get('timestamp')}")
    coins = Counter(r.get("coin") for r in subset)
    print("    coins", coins.most_common(8))
    spreads = [r.get("spread_pct") or 0 for r in subset]
    print(f"    spread min={min(spreads):.3f} max={max(spreads):.3f} avg={sum(spreads)/len(spreads):.3f}")
