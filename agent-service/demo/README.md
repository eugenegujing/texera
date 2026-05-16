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
| 7 | `outlier` | S041, S042, S044 | `bmi > 60` outside user-supplied `validRanges` (possible real extreme — flagged as `warning` tier) |

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

The frontend auto-trigger handles this in the real flow — once a `CSVFileScan`
operator is added that references this file, DataGuard auto-launches.

## Single-category demo files

For testing one detector at a time, each of these CSVs concentrates pollution
in a single category so it's obvious which detector is firing:

| File | Issue category | What's wrong |
|---|---|---|
| `missing_values_demo.csv` | `missing_value` | empty / `N/A` / `NA` / `null` cells across multiple columns |
| `placeholder_values_demo.csv` | `placeholder_value` | `999`, `-1`, `Unknown` / `unknown` sentinels |
| `duplicate_rows_demo.csv` | `duplicate_id` | repeated `sample_id`s, some with conflicting outcomes |
| `outliers_demo.csv` | `outlier` | negative ages, BMI > 200, blood pressure > 250 — fires only when `validRanges` is supplied at scan time |
| `inconsistent_labels_demo.csv` | `inconsistent_label` | `Male` / `male` / `MALE` and `Female` / `female` / `FEMALE` mixed |

The `outliers_demo.csv` requires `validRanges` to be set when scanning (the
profiler does not auto-detect numerical outliers via z-score — that variant was
removed because it flagged legitimate clustered extremes as errors). The other
four fire on default scan options.

Suggested `validRanges` for the outlier demo:

```json
{
  "age":            { "min": 0,   "max": 120 },
  "bmi":            { "min": 10,  "max": 60 },
  "blood_pressure": { "min": 40,  "max": 200 }
}
```

## Bias-check expectation

Group A: 22 rows. Group B: 23 rows. After cleaning, missingness imbalance
(more empties in A) means naive imputation drops ~18% of A but only ~4% of B
— DataGuard surfaces this and proposes a `replace_value` fix tagged with
`riskTier: "warning"` so the user explicitly confirms instead of letting
imputation run silently. The earlier `flag` operation kind was removed —
every fix is now a concrete change, and "please review manually" is conveyed
through the warning tier instead.
