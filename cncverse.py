#!/usr/bin/env python3
"""
CNCVerse Bridge (Stremio add-on) multi-provider lister — stdlib only.

Auto-discovers every 'tv' catalog in the manifest (they rotate/update, so always
fetched fresh) and resolves playable stream URLs for each provider.

Usage:
  python3 cncverse.py providers              # summary: catalogs + item counts + live counts
  python3 cncverse.py list                   # per-provider summary
  python3 cncverse.py list -p SportzX        # all items of provider matching 'SportzX'
  python3 cncverse.py list -f cricket        # filter by title/sport substring (all providers)
  python3 cncverse.py live                   # only currently LIVE events
  python3 cncverse.py streams 8              # resolve streams for item #8 from `list`
  python3 cncverse.py streams "redzone"      # resolve streams by title search
  python3 cncverse.py export -o streams      # all providers: streams.txt/.m3u/.json (links+keys)
  python3 cncverse.py export -o streams --live   # only status=LIVE items
  python3 cncverse.py export -p SportzX -o streams
"""
import argparse
import base64
import json
import re
import sys
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE = "https://cncverse.dpdns.org/u/p_82nkdh4pf87u"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"


def get_json(path):
    req = urllib.request.Request(f"{BASE}{path}", headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=40) as r:
        return json.loads(r.read().decode())


def b64fix(s):
    s = s.replace("-", "+").replace("_", "/")
    return s + "=" * (-len(s) % 4)


def decode_meta_id(mid):
    """Meta IDs look like cnc:<base64(ProviderName::{"eventId":N,...})>"""
    try:
        dec = base64.b64decode(b64fix(mid[4:]), validate=True).decode()
        return json.loads(dec[dec.index("{"):]) or {}
    except Exception:
        return {}


def status_from(meta_name, info):
    ev = info.get("eventInfo", {}) or {}
    if ev.get("isLive"):
        return "LIVE"
    if ev.get("isEnded"):
        return "ENDED"
    if meta_name.startswith("\U0001f534"):
        return "LIVE"
    if meta_name.startswith("\u2705"):
        return "ENDED"
    return "UPCOMING"


def get_tv_catalogs():
    m = get_json("/manifest.json")
    return [(c["id"], c["name"]) for c in m["catalogs"] if c["type"] == "tv"]


def load_events(cid=None):
    """Load events from ONE tv catalog (or the one in cid)."""
    cid = cid or "cnc_PlayZTVLiveEvents_tv"
    data = get_json(f"/catalog/tv/{cid}.json")
    events = []
    for m in data.get("metas", []):
        info = decode_meta_id(m["id"])
        ev = info.get("eventInfo", {}) or {}
        title = re.sub(r"^[🔴✅🔜]\s*", "", m.get("name", "")) or info.get("title") or "?"
        events.append({
            "raw_id": m["id"],
            "catalog_id": cid,
            "event_id": info.get("eventId"),
            "title": title.strip(),
            "status": status_from(m.get("name", ""), info),
            "sport": ev.get("eventType") or ev.get("eventCat") or "",
            "start": ev.get("startTime") or "",
        })
    return events


def load_all_events(catalogs, workers=8):
    """Load events from ALL tv catalogs in parallel; tag each with provider name."""
    events = []
    name_of = {cid: name for cid, name in catalogs}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(load_events, cid): cid for cid, _ in catalogs}
        for fut in as_completed(futs):
            cid = futs[fut]
            try:
                evs = fut.result()
                for e in evs:
                    e["provider"] = name_of[cid]
                events.extend(evs)
            except Exception as ex_:  # noqa
                print(f"  ! {name_of.get(cid, cid)} failed: {ex_}", file=sys.stderr)
    return events


def resolve_streams(ev):
    path = f"/stream/tv/{urllib.parse.quote(ev['raw_id'], safe='')}.json"
    try:
        data = get_json(path)
    except Exception as e:
        return [{"name": "ERROR", "title": str(e), "url": "", }]
    return data.get("streams", [])


def stream_type(url):
    """Classify a stream URL: hls (browser-playable), drm (HLS w/ clearkey),
    dash, file (direct mp4/..., usually playable), other."""
    u = url or ""
    if ".m3u8" in u:
        return "drm" if "clearkey" in u else "hls"
    if ".mpd" in u:
        return "dash"
    if re.search(r"\.(mp4|ts|m3u|aac|mp3)(\?|$)", u, re.I):
        return "file"
    return "other"


def fmt_stream_row(s):
    name = s.get("name", "")
    title = re.sub(r"\s+", " ", s.get("title", "")).split("\n")[0]
    url = s.get("url") or s.get("externalUrl") or ""
    if url == "Ok":  # bridge-side placeholder when upstream resolution failed
        url = ""
    quality = ""
    q = re.search(r"(2160p|1440p|1080p|720p|576p|540p|480p|404p|396p|360p|Auto)", title)
    if q:
        quality = q.group(1)
    return name, title, quality, url


def event_rows(events, only_live=False, flt=None, provider=None, category=None):
    rows = list(events)
    if only_live:
        rows = [e for e in rows if e["status"] == "LIVE"]
    if provider:
        p = provider.lower()
        rows = [e for e in rows if p in e["provider"].lower()]
    if flt:
        f = flt.lower()
        rows = [e for e in rows if f in e["title"].lower() or f in e["sport"].lower()]
    rows = [e for e in rows if category_matches(e, category)]
    order = {"LIVE": 0, "UPCOMING": 1, "ENDED": 2}
    rows.sort(key=lambda e: (order[e["status"]], e["provider"], -(e["event_id"] or 0)))
    return rows


def summary(events, catalogs=None):
    from collections import defaultdict, Counter
    stats = defaultdict(lambda: [0, 0, 0])
    for e in events:
        s = stats[e["provider"]]
        if e["status"] == "LIVE":
            s[0] += 1
        elif e["status"] == "UPCOMING":
            s[1] += 1
        else:
            s[2] += 1
    # category breakdown per provider
    prov_cats = defaultdict(Counter)
    for e in events:
        cat = (e["sport"] or "unknown").strip()
        prov_cats[e["provider"]][cat.title()] += 1
    print(f"{'PROVIDER':35} {'LIVE':>5} {'UPC':>5} {'END':>5} {' TOTAL':>6}")
    for prov in sorted(stats, key=lambda k: k.lower()):
        l, u, d = stats[prov]
        print(f"{prov:35} {l:>5} {u:>5} {d:>5} {l+u+d:>6}")
        top = ", ".join(f"{c} ({n})" for c, n in prov_cats[prov].most_common(5))
        print(f"{'':35}   categories: {top}")
    tot = [sum(v[i] for v in stats.values()) for i in range(3)]
    print("-" * 56)
    print(f"{'TOTAL':35} {tot[0]:>5} {tot[1]:>5} {tot[2]:>5} {sum(tot):>6}")
    print()
    global_cats = Counter((e["sport"] or "unknown").title() for e in events)
    print("ALL CATEGORIES:")
    for c, n in global_cats.most_common():
        live = sum(1 for e in events if (e["sport"] or "unknown").title() == c and e["status"] == "LIVE")
        print(f"  {c:30} {n:>5}  ({live} live)")


def category_matches(ev, cat):
    if not cat:
        return True
    c = cat.lower()
    return c in (ev["sport"] or "").lower()


def print_events(rows, numbered=True):
    if not rows:
        print("No events matched.")
        return
    if numbered:
        print(f"{'#':>3}  {'STATUS':8} PROVIDER{chr(32)*24} INFO")
    for i, e in enumerate(rows, 1):
        n = f"{i:>3}  " if numbered else ""
        prov = (e.get("provider") or "?")[:32]
        print(f"{n}{e['status']:8} {prov:32} {e['title']}  "
              f"[{e['sport'].title() or '?'} | {e['start'] or 'no time'}]")
    if numbered:
        print("\nResolve:  python3 cncverse.py streams <# from this list, or title text>")


def print_streams(ev, streams):
    print(f"\n=== {ev['title']}  [{ev.get('provider','?')} | ID {ev.get('event_id')} | {ev['status']}] — {len(streams)} stream(s) ===\n")
    for s in streams:
        name, title, quality, url = fmt_stream_row(s)
        if not url.startswith("http"):
            print(f"[{quality or name}] {title}  (no direct url)")
            continue
        print(f"[{quality or name}] {title}")
        print(f"    URL: {url}")
        if s.get("clearkey"):
            print(f"    clearkey: {s['clearkey']}")
        print()
    print("Play with:  ffplay -user_agent '<UA>' '<URL>'     # or open in VLC / mpv")


def export_streams(rows, outdir="out", workers=8):
    """Resolve streams for every row (parallel) and write streams.txt / .m3u / .json."""
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)

    # resolve all streams in parallel
    results = [None] * len(rows)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(resolve_streams, ev): i for i, ev in enumerate(rows)}
        done = 0
        for fut in as_completed(futs):
            results[futs[fut]] = fut.result()
            done += 1
            if done % 50 == 0:
                print(f"  resolved {done}/{len(rows)}...", file=sys.stderr)

    txt_lines = ["# CNCVerse live streams (all providers) — generated by cncverse.py", ""]
    m3u = ["#EXTM3U"]
    json_events = []
    seen_urls = set()
    count = 0
    for ev, streams in zip(rows, results):
        got = 0
        event_json = {**ev, "streams": []}
        for s in streams:
            name, title, quality, url = fmt_stream_row(s)
            if not url.startswith("http") or url in seen_urls:
                continue
            seen_urls.add(url)
            got += 1
            count += 1
            ck = s.get("clearkey", "")
            txt_lines.append(f"[{ev['provider']} | {ev['status']}] {ev['title']}")
            txt_lines.append(f"  Quality : {quality or '?'}")
            txt_lines.append(f"  URL     : {url}")
            if ck:
                txt_lines.append(f"  CLEARKEY: {ck}")
            txt_lines.append("")
            m3u.append(f'#EXTINF:-1 tvg-name="{ev["title"]}",{ev["title"]} [{ev["provider"]} | {quality or "auto"}]')
            m3u.append(url)
            event_json["streams"].append({"quality": quality or name, "url": url,
                                          "clearkey": ck, "type": stream_type(url)})
        if got:
            json_events.append(event_json)

    (out / "streams.txt").write_text("\n".join(txt_lines) + "\n", encoding="utf-8")
    (out / "streams.m3u").write_text("\n".join(m3u) + "\n", encoding="utf-8")
    (out / "streams.json").write_text(json.dumps(json_events, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {out/'streams.txt'}, {out/'streams.m3u'}, {out/'streams.json'} — {count} unique streams from {len(json_events)} events")


def main():
    ap = argparse.ArgumentParser(description="CNCVerse Bridge (Stremio) multi-provider live TV tool")
    ap.add_argument("command", choices=["providers", "summary", "list", "live", "streams", "export"], help="action")
    ap.add_argument("query", nargs="?", default="", help="item number (from list) or title substring, for 'streams'")
    ap.add_argument("-f", "--filter", default=None, help="filter by title/sport substring")
    ap.add_argument("-p", "--provider", default=None, help="limit to provider(s) whose name matches substring")
    ap.add_argument("-c", "--category", default=None, help="limit to category/sport whose name matches (e.g. cricket, football, wwe)")
    ap.add_argument("-o", "--out", default="out", help="output directory for 'export'")
    ap.add_argument("-w", "--workers", type=int, default=8, help="parallel workers (export)")
    ap.add_argument("--live", action="store_true", help="export: only status=LIVE events")
    args = ap.parse_args()

    catalogs = get_tv_catalogs()

    if args.command == "providers":
        print(f"{len(catalogs)} tv catalogs:")
        for cid, name in catalogs:
            print(f"  {cid:35} {name}")
        events = load_all_events(catalogs, args.workers)
        summary(events)

    elif args.command == "summary":
        events = load_all_events(catalogs, args.workers)
        summary(events)

    elif args.command in ("list", "live") and not args.provider and not args.filter and not args.category:
        # bare list = per-provider overview, then full listing
        events = load_all_events(catalogs, args.workers)
        summary(events)
        print()
        print_events(event_rows(events, only_live=(args.command == "live")))

    elif args.command in ("list", "live"):
        events = load_all_events(catalogs, args.workers)
        print_events(event_rows(events, only_live=(args.command == "live"),
                                flt=args.filter, provider=args.provider,
                                category=args.category))

    elif args.command == "export":
        events = load_all_events(catalogs, args.workers)
        rows = event_rows(events, flt=args.filter, provider=args.provider,
                          category=args.category)
        if args.live:
            rows = [e for e in rows if e["status"] == "LIVE"]
        print(f"Resolving streams for {len(rows)} items across "
              f"{len({e['provider'] for e in rows})} provider(s)...")
        export_streams(rows, outdir=args.out, workers=args.workers)

    elif args.command == "streams":
        events = load_all_events(catalogs, args.workers)
        rows = event_rows(events, flt=args.filter, provider=args.provider,
                          category=args.category)
        q = args.query.strip()
        if q.isdigit():
            idx = int(q)
            if not 1 <= idx <= len(rows):
                sys.exit(f"index {idx} out of range ({len(rows)} items)")
            sel = rows[idx - 1]
        elif q:
            hits = [e for e in rows if q.lower() in e["title"].lower()]
            if not hits:
                sys.exit(f"No event matching {q!r}")
            if len(hits) > 1:
                print("Ambiguous — matches:")
                for i, h in enumerate(hits, 1):
                    print(f"  {i:>3}  {h['status']:8} {h['provider'][:28]:28} {h['title']}")
                sys.exit("Pass a number instead, or narrow with -p / -f.")
            sel = hits[0]
        else:
            sys.exit("streams: pass an item number (from list) or title text")
        print(f"Working: {sel['title']}")
        print_streams(sel, resolve_streams(sel))


if __name__ == "__main__":
    main()
