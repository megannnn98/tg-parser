# Retrieval evaluation set

22 queries over the comments of the five profiles with the most comments.
Each query belongs to one user: the benchmark searches that user's comments
only, which is how a profile is analysed.

## Files

| File | In git | Content |
|---|---|---|
| `queries.jsonl` | yes | query id, anonymous user, topic, query text, keyword pattern of the topic |
| `judgments.jsonl` | yes | manual additions to and removals from the pattern matches |
| `dataset.jsonl` | yes | the relevant messages of every query |
| `mapping.local.json` | **no** | which Telegram user and comment each anonymous id stands for |
| `build.py` | yes | rebuilds `dataset.jsonl` from the three above and the database |

Nothing in git names a user, a channel or a Telegram message id, and no
comment text is stored. `eval_user_NN` and `eval_msg_NNNN` mean something only
together with `mapping.local.json`, which stays on the machine that holds the
comments. A `(channel, message id)` pair would point at a real person's
comment, so it is kept out of the repository.

## How relevance was decided

1. A message is relevant to a query when it matches the query's `pattern`
   (case-insensitive regular expression). This is lexical, so it does not
   depend on any embedding model and cannot favour one chunking over another.
2. The 12 best message-level E5 hits of every query were read. Relevant ones
   that the pattern misses were added by hand (20 messages), and one false
   match was removed. These are in `judgments.jsonl`.

## Limits

- The pattern matches were not all read. A match means the message mentions
  the topic, not that it states a position on it. The share of false matches
  is not measured; in the one query read closely it was about one in five.
- The manual additions come from message-level retrieval, so they lean
  slightly towards it.
- Replies that are on topic only through their context ("да", "согласен")
  are not labelled. Recall therefore says nothing about them; the benchmark
  reports the share of very short messages among the results instead.
- Queries have 17 to 305 relevant messages, so Recall@5 and Recall@10 are
  small numbers by construction. They compare strategies with each other.

## Reproducing

With `mapping.local.json` and the same comments in PostgreSQL:

    python -m benchmarks.eval.build build

rewrites `dataset.jsonl` identically: ids once assigned are never changed, new
ones are appended. Without the mapping the dataset cannot be resolved to
comments, and the retrieval benchmark does not run; that is the price of
keeping the comments private.

To extend it: `counts` shows how many messages each pattern matches, `show`
prints candidates for manual judgment (real comments, to the terminal only),
`add` and `remove` record a judgment.
