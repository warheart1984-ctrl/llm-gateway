#!/usr/bin/env python3
"""Rescore fold_tools result JSONs: strict vs lenient accuracy (PR #67 lenient_match
logic), format-only misses, provider cost, and per-fold requests served after commit.
Usage: python tools/score_fold_tools.py benchmark_results/fold_tools_*.json"""
import json, re, sys
_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")
def strict_match(v, g): return type(v) is type(g) and v == g
def lenient_match(v, g):
    if strict_match(v, g): return True
    if isinstance(g, bool) or not isinstance(g, int) or isinstance(v, bool) or not isinstance(v, str): return False
    t = v.strip()
    if t == str(g): return True
    if len(t) > 160 or len(t.split()) > 24: return False
    n = _NUMBER.findall(t)
    if len(n) != 1 or "." in n[0] or n[0] != str(int(n[0])): return False
    return int(n[0]) == g
def score(path):
    d = json.load(open(path)); calls = d["calls"]; s = d["settings"]
    print(f"== {path}\n max_calls={s['max_calls']} commit={s['git_commit'][:7]}")
    for arm in ("plain", "folded"):
        asked = [c for c in calls if c.get("question")]
        st = sum(strict_match(c[arm].get("answer"), c["gold"]) for c in asked)
        le = sum(lenient_match(c[arm].get("answer"), c["gold"]) for c in asked)
        print(f" {arm}: strict {st}/{len(asked)}={st/len(asked):.1%} lenient {le}/{len(asked)}={le/len(asked):.1%} format_only_misses {le-st}")
    el = [c for c in calls if c.get("eligible")]
    st = sum(strict_match(c["folded"].get("answer"), c["gold"]) for c in el)
    le = sum(lenient_match(c["folded"].get("answer"), c["gold"]) for c in el)
    print(f" eligible folded: strict {st}/{len(el)} lenient {le}/{len(el)}")
    a = d.get("aggregates")
    if not a: print(" NO AGGREGATES:", d.get("error")); return
    p = a["caching"]["provider_cost_usd"]; cs = a["caching"]["cached_share"]
    print(f" plain ${p['plain']:.4f} folded_main ${p['folded_main_calls']:.4f} summarizer ${p['folded_summarizer_calls']:.4f} total ${p['folded']:.4f} net {p['saving_share']:.1%}")
    print(f" folds {a['folds']} cached plain {cs['plain']:.1%} folded {cs['folded']:.1%}")
    commits = [c for c in calls if c["folded"].get("fold_decision", {}).get("outcome") == "committed"] or []
    fs = [f["call"] for f in d["fold_summaries"]]
    ends = fs[1:] + [len(calls) + 1]
    for i, (k, e) in enumerate(zip(fs, ends)):
        row = next((c for c in calls if c["call"] == k), {}); fd = row.get("folded", {}).get("fold_decision", {})
        served_any = len(calls) - k  # calls after commit (prefix still reused under later folds)
        print(f" fold {i+1}: committed at call {k}, served {e-k-1} calls before next fold, {served_any} to end; "
              f"tokens_cut {fd.get('prompt_tokens_cut')} est_benefit ${fd.get('benefit_nano_usd',0)/1e9:.5f} overhead ${fd.get('overhead_nano_usd',0)/1e9:.5f} outcome {fd.get('outcome')}")
for f in sys.argv[1:]: score(f)
