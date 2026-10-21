"""The official OpenAI Python SDK, pointed at the gateway by base URL only.

    python openai_sdk_check.py --base-url http://127.0.0.1:8080/v1

Uses RECEIPTS_DEMO_KEY as the API key. Checks a completion, a stream with
usage, the receipt headers, and that refusals surface as the SDK's own
exceptions. Exits non-zero on the first failed check.
"""

from __future__ import annotations

import argparse
import os
import sys

import openai


def check(condition: bool, text: str) -> None:
    if not condition:
        print(f"   FAIL {text}", flush=True)
        sys.exit(1)
    print(f"   ok  {text}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    args = parser.parse_args()
    key = os.environ["RECEIPTS_DEMO_KEY"]
    client = openai.OpenAI(base_url=args.base_url, api_key=key)
    messages = [{"role": "user", "content": "Say something about receipts."}]

    print(f"\n== The OpenAI SDK ({openai.__version__}) with only base_url changed", flush=True)
    raw = client.chat.completions.with_raw_response.create(model="mock/chat", messages=messages, max_tokens=32)
    completion = raw.parse()
    check(completion.object == "chat.completion" and completion.id.startswith("chatcmpl-"),
          f"chat.completions.create returned {completion.id[:20]}...")
    check(bool(completion.choices[0].message.content), f"answer: {completion.choices[0].message.content[:60]}...")
    check(completion.usage is not None and completion.usage.total_tokens > 0, "usage is reported")
    check(raw.headers.get("x-receipt-id", "").startswith("receipt:"), "and the gateway's receipt id rides along in a header")

    stream = client.chat.completions.create(model="mock/chat", messages=messages, max_tokens=32, stream=True,
                                            stream_options={"include_usage": True})
    text, usage, finish = "", None, None
    for chunk in stream:
        if chunk.choices:
            text += chunk.choices[0].delta.content or ""
            finish = chunk.choices[0].finish_reason or finish
        if chunk.usage:
            usage = chunk.usage
    check(bool(text) and finish is not None, f"streaming works: {len(text)} characters, finish_reason={finish}")
    check(usage is not None and usage.total_tokens > 0, "the final usage chunk arrives with stream_options.include_usage")

    models = client.models.list()
    check(any(m.id == "mock/chat" for m in models.data), "models.list returns mock/chat")
    check(all(m.object == "model" for m in models.data), "every model entry has object model")
    check(all(m.created is not None for m in models.data), "every model entry has created")
    check(all(m.owned_by for m in models.data), "every model entry has owned_by")

    try:
        openai.OpenAI(base_url=args.base_url, api_key="not-a-key").chat.completions.create(
            model="mock/chat", messages=messages, max_tokens=8)
        check(False, "a bad key must be refused")
    except openai.AuthenticationError as error:
        check(error.status_code == 401, "a bad key raises the SDK's AuthenticationError")
    try:
        client.chat.completions.create(model="mock/chat", messages=messages, max_tokens=8, n=2)
        check(False, "n=2 must be refused")
    except openai.BadRequestError as error:
        check(error.status_code == 400, "an unsupported parameter raises BadRequestError")
    print("All SDK checks passed.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
