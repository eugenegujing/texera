# DataGuard demo dataset

`diabetes_messy.csv` — a deliberately polluted ~50-row sample modeled on the
UCI Pima Indians Diabetes dataset. Each row is one subject; the rightmost
column (`diabetic_outcome`) is the label.

## Injected issues (every issue type DataGuard detects)

| # | Issue type | Rows | What's wrong |
|---|---|---|---|
| 1 | `placeholder_value` | S004, S012 | `age = 999` (sentinel) |
| 2 | `placeholder_value` | S043 | `bmi = -1` (sentinel) |
| 3 | `placeholder_value` | S047 | `age = "Unknown"` (string sentinel) |
| 4 | `missing_value` | S005, S007, S009, S014 | empty `glucose` in Group A (imbalanced) |
| 5 | `missing_value` | S045, S046, S048 | `age = "N/A"`, `glucose = " "`, `glucose = "null"` |
| 6 | `duplicate_id` | S001, S017 | sample_id repeats with conflicting `diabetic_outcome` |
| 7 | `out_of_range` | S041, S042, S044 | `bmi > 60` (clinical max ~70 — possibly real, possibly error) |

## How to load into DataGuard

```bash
# After bun install + bun run dev in agent-service/
curl -X POST http://localhost:8000/api/agents/<agentId>/dataguard/dataset \
  -H "Content-Type: application/json" \
  -d "$(jq -nR --rawfile c diabetes_messy.csv '{
    columns: ($c | split("\n")[0] | split(",")),
    rows: ($c | split("\n")[1:] | map(split(",") | length as $n | reduce range(0;$n) as $i ({}; . + {(.|keys|"col\($i)"): .[$i]})))
  }')"
```

Or via the demo script (Step 13's frontend auto-trigger handles this in the
real flow — once a `CSVFileScan` operator is added that references this file,
DataGuard auto-launches).

## Bias-check expectation

Group A: 22 rows. Group B: 23 rows. After cleaning, missingness imbalance
(more empties in A) means naive imputation drops ~18% of A but only ~4% of B
— DataGuard surfaces this and recommends `flag` instead of `impute` for the
missing-glucose issue (the §5 storyboard "Modify" beat).
