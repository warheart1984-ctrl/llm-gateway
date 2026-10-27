"""Replay each folded format-only miss's captured forwarded body to OpenRouter N times."""
import json, os, sys, urllib.request, glob
sys.path.insert(0, os.path.dirname(__file__))
from fold_tools import parse_answer, strict_match, lenient_match

def post(body):
    req = urllib.request.Request("https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps(body).encode(), headers={"Authorization": "Bearer " + os.environ["OPENROUTER_API_KEY"], "Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=120))

def main(results, n=9, cap=0.25):
    spent, out = 0.0, []
    for path in results:
        d = json.load(open(path))
        side = {}
        for line in open(path.replace(".json", ".forwarded.jsonl")):
            j = json.loads(line); side[j["request_id"]] = j
        for c in d["calls"]:
            f = c["folded"]
            if f["correct"] or not f["lenient_correct"]:
                continue
            cap_body = dict(side[f["request_id"]]["body"]); cap_body["usage"] = {"include": True}
            runs = []
            for _ in range(n):
                if spent > cap: break
                r = post(cap_body); spent += r.get("usage", {}).get("cost", 0) or 0
                ch = r["choices"][0]; v, ok = parse_answer(ch["message"].get("content"), ch.get("finish_reason"))
                runs.append({"answer": v, "strict": bool(ok and strict_match(v, c["gold"])), "lenient": bool(ok and lenient_match(v, c["gold"]))})
            out.append({"result": os.path.basename(path), "call": c["call"], "gold": c["gold"], "run_answer": f["answer"],
                        "temperature": cap_body.get("temperature"), "replays": runs,
                        "strict": sum(x["strict"] for x in runs), "lenient": sum(x["lenient"] for x in runs)})
            print(out[-1]["result"][-40:], c["call"], f"strict {out[-1]['strict']}/{len(runs)} lenient {out[-1]['lenient']}/{len(runs)}")
    print(f"replay spend ${spent:.4f}")
    json.dump({"model_body": "captured forwarded body, unchanged", "spend_usd": spent, "misses": out},
              open("benchmark_results/replay_forwarded_misses_20261027.json", "w"), indent=1)

if __name__ == "__main__":
    main(sys.argv[1:])
