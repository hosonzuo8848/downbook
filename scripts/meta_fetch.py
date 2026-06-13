import os, sys, csv, json, time, asyncio, argparse, re
import aiohttp
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

CFG = {
  "bnf_dunhuang":{"src":"lists/bnf_dunhuang.csv","kind":"iiif"},
  "codh":        {"src":"lists/codh.csv",        "kind":"iiif"},
  "princeton":   {"src":"lists/princeton.csv",   "kind":"iiif"},
  "nijl":        {"src":"lists/nijl.csv",        "kind":"iiif"},
  "nagoya":      {"src":"lists/nagoya.csv",      "kind":"iiif"},
  "bodleian":    {"src":"lists/bodleian.csv",    "kind":"iiif"},
  "loc":         {"src":"lists/loc.csv",         "kind":"iiif"},
  "toyo":        {"src":"lists/toyo.csv",        "kind":"iiif"},
  "taiwan":      {"src":"lists/taiwan.csv",      "kind":"iiif"},
  "cambridge":   {"src":"lists/cambridge.csv",   "kind":"iiif"},
  "berlin_un":   {"src":"lists/berlin_un.csv",   "kind":"iiif"},
  "vietnam":     {"src":"lists/vietnam.csv",     "kind":"iiif"},
}
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

def extract_iiif(d):
    o = {"_label":"", "_canvas":-1, "_attribution":"", "_license":""}
    L = d.get("label","")
    if isinstance(L,list): L = L[0] if L else ""
    if isinstance(L,dict): L = next(iter(L.values()),"")
    if isinstance(L,list): L = L[0] if L else ""
    o["_label"] = str(L)[:200]
    n = 0
    for s in d.get("sequences",[]): n += len(s.get("canvases",[]))
    if n == 0: n = len(d.get("items",[]))
    o["_canvas"] = n
    o["_attribution"] = str(d.get("attribution",""))[:200]
    lic = d.get("license","")
    if isinstance(lic,list): lic = lic[0] if lic else ""
    o["_license"] = str(lic)[:120]
    for m in d.get("metadata",[]):
        k = m.get("label",""); v = m.get("value","")
        if isinstance(k,list): k = k[0] if k else ""
        if isinstance(k,dict): k = next(iter(k.values()),"")
        if isinstance(k,list): k = k[0] if k else ""
        if isinstance(v,list): v = "; ".join(str(x) for x in v) if v else ""
        if isinstance(v,dict): v = next(iter(v.values()),"")
        if isinstance(v,list): v = "; ".join(str(x) for x in v) if v else ""
        o[str(k)] = str(v)[:300]
    return o

def extract_waseda(html):
    o = {"_canvas":-1}
    for m in re.finditer(r'<DIV ID="(\w+)"[^>]*>([\s\S]+?)</DIV>', html):
        k = m.group(1); v = re.sub(r"<[^>]+>","", m.group(2)).strip()
        v = re.sub(r"\s+"," ", v)
        o[k] = v[:300]
    tm = re.search(r"<TITLE>\s*([\s\S]+?)\s*</TITLE>", html)
    if tm: o["_title_tag"] = tm.group(1).strip()[:300]
    im = re.search(r'SRC="(https://archive\.wul[^"]+)"', html)
    if im: o["_thumb"] = im.group(1)
    return o

def waseda_url(rid, cls):
    return f"https://www.wul.waseda.ac.jp/kotenseki/html/{cls}/{rid}/index.html"

async def one(s, url, kind, sem, retries=3):
    last = "?"
    async with sem:
        for k in range(retries):
            try:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    if r.status == 200:
                        if kind == "iiif":
                            return extract_iiif(await r.json(content_type=None)), "ok"
                        return extract_waseda(await r.text()), "ok"
                    last = f"HTTP{r.status}"
            except Exception as e:
                last = type(e).__name__
            await asyncio.sleep(0.3 + k*0.5)
        return {}, last

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lib", required=True)
    ap.add_argument("--conc", type=int, default=50)
    a = ap.parse_args()
    c = CFG[a.lib]
    rows = list(csv.DictReader(open(c["src"], encoding="utf-8-sig")))
    print(f"[{a.lib}] {len(rows)} 行 sem={a.conc}", flush=True)
    os.makedirs("out", exist_ok=True)
    fh = open(f"out/{a.lib}_meta.jsonl", "w", encoding="utf-8", buffering=1)
    sem = asyncio.Semaphore(a.conc)
    done = failed = 0
    t0 = time.time()
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=a.conc+50, ssl=False), headers={"User-Agent":UA}) as s:
        async def task(r):
            nonlocal done, failed
            rid = (r.get("id") or "").strip()
            if not rid: return
            if a.lib == "waseda":
                cls = (r.get("cls") or "").strip()
                if not cls: return
                url = waseda_url(rid, cls)
            else:
                url = (r.get("manifest") or "").strip()
                if not url: return
            meta, st = await one(s, url, c["kind"], sem)
            done += 1
            if st != "ok": failed += 1
            fh.write(json.dumps({"id":rid,"status":st,"meta":meta}, ensure_ascii=False) + "\n")
            if done % 500 == 0:
                rate = done/(time.time()-t0); eta = (len(rows)-done)/rate if rate>0 else 0
                print(f"  {done}/{len(rows)} 失败 {failed} {rate:.1f}/s ETA {eta/60:.1f}m", flush=True)
        await asyncio.gather(*[task(r) for r in rows])
    fh.close()
    print(f"[{a.lib}] 完 失败 {failed}/{done}", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
