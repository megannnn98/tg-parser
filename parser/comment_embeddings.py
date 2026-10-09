"""Local E5 embeddings for full comments, including texts longer than 512 tokens."""
from parser.position_inference import LocalE5


class LocalCommentE5(LocalE5):
    def __init__(self, cache_dir=None):
        super().__init__(cache_dir)
        self.version = f"{self.model_name}@{self.revision}:query:token-chunks:weighted-mean:l2:v1"

    def _encode_locked(self, texts):
        import torch

        self._load_model()
        tokenizer = self._tokenizer
        prefix = tokenizer.encode("query: ", add_special_tokens=False)
        capacity = 512 - len(prefix) - tokenizer.num_special_tokens_to_add(pair=False)
        chunks = []
        for index, text in enumerate(texts):
            tokens = tokenizer.encode(text, add_special_tokens=False)
            chunks.extend((index, tokens[i:i + capacity]) for i in range(0, len(tokens), capacity))
            if not tokens:
                chunks.append((index, []))
        totals = {}
        for start in range(0, len(chunks), 16):
            batch = chunks[start:start + 16]
            # The special tokens are added by hand: transformers 5 dropped prepare_for_model.
            rows = [
                [tokenizer.cls_token_id, *prefix, *chunk, tokenizer.sep_token_id]
                for _, chunk in batch
            ]
            rows = [{"input_ids": row, "attention_mask": [1] * len(row)} for row in rows]
            inputs = tokenizer.pad(rows, padding=True, return_tensors="pt").to(self._model.device)
            with torch.inference_mode():
                output = self._model(**inputs).last_hidden_state
                mask = inputs.attention_mask[..., None]
                pooled = (output * mask).sum(1) / mask.sum(1)
                vectors = torch.nn.functional.normalize(pooled, p=2, dim=1)
                for (index, chunk), vector in zip(batch, vectors):
                    weighted = vector * max(1, len(chunk))
                    totals[index] = totals[index] + weighted if index in totals else weighted
        return [torch.nn.functional.normalize(totals[i], p=2, dim=0).cpu().tolist() for i in range(len(texts))]
