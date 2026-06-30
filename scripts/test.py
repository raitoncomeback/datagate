from openai import OpenAI
import os, time
from dotenv import load_dotenv
load_dotenv()

client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=os.environ["OPENROUTER_API_KEY"])

models_to_test = [
    "openai/gpt-oss-20b:free",
    "openai/gpt-oss-120b:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
    "qwen/qwen3-coder:free",
    "google/gemma-4-26b-a4b-it:free",
    "nousresearch/hermes-3-llama-3.1-405b:free",
]

prompt = "Return ONLY valid JSON, no explanation: {\"sentiment\": \"bullish\", \"confidence\": 0.8}"

for model in models_to_test:
    try:
        start = time.time()
        r = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            timeout=30,
        )
        elapsed = time.time() - start
        content = r.choices[0].message.content.strip()
        print(f"{elapsed:.1f}s OK — {model} — {content[:60]}")
    except Exception as e:
        print(f"FAIL — {model} — {str(e)[:100]}")