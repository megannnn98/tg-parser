import pytest

torch = pytest.importorskip("torch")

from parser.comment_embeddings import LocalCommentE5  # noqa: E402


class _Batch(dict):
    def to(self, device):
        return self

    @property
    def attention_mask(self):
        return self["attention_mask"]


class FakeTokenizer:
    """Offers only what a transformers 5 tokenizer still has."""

    cls_token_id = 0
    sep_token_id = 2

    def __init__(self):
        self.padded: list[list[dict]] = []

    def encode(self, text, add_special_tokens=False):
        assert add_special_tokens is False
        return [3 + ord(char) % 5 for char in text.replace(" ", "")]

    def num_special_tokens_to_add(self, pair=False):
        return 2

    def pad(self, rows, padding=True, return_tensors="pt"):
        self.padded.append(rows)
        width = max(len(row["input_ids"]) for row in rows)
        return _Batch(
            input_ids=torch.tensor(
                [row["input_ids"] + [1] * (width - len(row["input_ids"])) for row in rows]
            ),
            attention_mask=torch.tensor(
                [row["attention_mask"] + [0] * (width - len(row["attention_mask"])) for row in rows]
            ),
        )


class FakeModel:
    device = "cpu"

    def __call__(self, input_ids, attention_mask):
        hidden = torch.nn.functional.one_hot(input_ids, 8).float()
        return type("Output", (), {"last_hidden_state": hidden})()


def _embedder() -> tuple[LocalCommentE5, FakeTokenizer]:
    embedder = LocalCommentE5()
    tokenizer = FakeTokenizer()
    embedder._tokenizer, embedder._model = tokenizer, FakeModel()
    embedder._load_model = lambda: None
    return embedder, tokenizer


def test_each_row_is_wrapped_in_the_special_tokens():
    embedder, tokenizer = _embedder()

    vectors = embedder._encode_locked(["abc"])

    prefix = tokenizer.encode("query: ")
    row = tokenizer.padded[0][0]
    assert row["input_ids"] == [0, *prefix, *tokenizer.encode("abc"), 2]
    assert row["attention_mask"] == [1] * len(row["input_ids"])
    assert sum(value * value for value in vectors[0]) == pytest.approx(1.0)


def test_a_long_comment_is_split_into_rows_of_at_most_512_tokens():
    embedder, tokenizer = _embedder()

    vectors = embedder._encode_locked(["a" * 1200, ""])

    lengths = [len(row["input_ids"]) for rows in tokenizer.padded for row in rows]
    assert max(lengths) == 512
    assert len(lengths) == 4
    assert len(vectors) == 2
