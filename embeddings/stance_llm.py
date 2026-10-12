"""Strict local Ollama verdict for a retrieved pair of Russian comments."""
from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class LlmJudgement:
    relation: str
    confidence: float
    sarcasm: bool


class OllamaStanceVerifier:
    def __init__(self, url: str, model: str = "qwen3:8b", timeout: int = 180):
        self.url, self.model, self.timeout = url.rstrip("/"), model, timeout
        self.spec = type("Spec", (), {"name": f"ollama/{model}", "revision": "local"})()

    def __call__(self, pairs: list[tuple[str, str]]) -> list[LlmJudgement]:
        return [self._one(left, right) for left, right in pairs]

    def _one(self, left: str, right: str) -> LlmJudgement:
        prompt = f'''Сравни политическую позицию авторов двух комментариев. Учитывай отрицания, цитирование и сарказм. Не считай общую тему согласием.
Верни только JSON: {{"relation":"contradiction"|"unclear","confidence":0..1,"sarcasm":true|false}}.
Комментарий A: {left}\nКомментарий B: {right}'''
        body = json.dumps({"model": self.model, "prompt": prompt, "stream": False, "format": "json", "think": False}).encode()
        with urlopen(Request(f"{self.url}/api/generate", data=body, headers={"Content-Type": "application/json"}), timeout=self.timeout) as response:
            data = json.loads(response.read())
        verdict = json.loads(data["response"])
        relation = "contradiction" if verdict.get("relation") == "contradiction" else "unclear"
        return LlmJudgement(relation, max(0, min(1, float(verdict.get("confidence", 0)))), bool(verdict.get("sarcasm")))
