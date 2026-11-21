# Scale run evidence

One end-to-end run of this pipeline on Project 2's full customer base, verified on 2026-11-20.

- **Scope:** synthetic data, one local machine (16 threads), not production.
- **Source:** a local verification script, kept outside this repository, compared every stage with the one before it. This file lists its 31 checks with the counts it printed. Counts only: no data rows.
- **Result:** 31 passed, 0 failed.
- **Not covered here:** CI does not run at this size. CI runs the tests at the default size (the committed export, about 500 customers). The walk of `GET /features` over all 383965 users was a separate check and is not one of the 31.

The settings the run needed are in the README, under "Running at scale".

## Kafka and Bronze

| # | Check | Result | Actual | Expected |
|---:|---|---|---:|---:|
| 1 | producer delivered == Kafka user_events end offsets | PASS | 3971944 | 3971944 |
| 2 | Bronze rows == Kafka user_events end offsets | PASS | 3971944 | 3971944 |
| 3 | Bronze distinct event_id == Bronze rows | PASS | 3971944 | 3971944 |
| 4 | DLQ end offsets | PASS | 0 | 0 |

## Dims and users

| # | Check | Result | Actual | Expected |
|---:|---|---|---:|---:|
| 5 | users dim rows == export customers | PASS | 399997 | 399997 |
| 6 | export customers.csv rows | PASS | 399997 | 399997 |
| 7 | active users in the dim == producer users | PASS | 383965 | 383965 |
| 8 | Bronze distinct session_id == session target | PASS | 652740 | 652740 |
| 9 | Bronze distinct user_id == active users | PASS | 383965 | 383965 |
| 10 | Bronze users that are not active | PASS | 0 | 0 |
| 11 | active users without an event | PASS | 0 | 0 |

## Silver

| # | Check | Result | Actual | Expected |
|---:|---|---|---:|---:|
| 12 | Silver rows == Bronze rows | PASS | 3971944 | 3971944 |
| 13 | Silver distinct event_id == Silver rows | PASS | 3971944 | 3971944 |
| 14 | Silver missing user references | PASS | 0 | 0 |
| 15 | Silver missing product references | PASS | 0 | 0 |
| 16 | Silver report bronze_rows | PASS | 3971944 | 3971944 |
| 17 | Silver report rows_dropped_by_filters | PASS | 0 | 0 |
| 18 | Silver report duplicates_removed | PASS | 0 | 0 |
| 19 | Silver report silver_record_count | PASS | 3971944 | 3971944 |

## Gold

| # | Check | Result | Actual | Expected |
|---:|---|---|---:|---:|
| 20 | Gold rows == active users | PASS | 383965 | 383965 |
| 21 | Gold distinct user_id == Gold rows | PASS | 383965 | 383965 |
| 22 | Gold sum(total_events) == Bronze rows | PASS | 3971944 | 3971944 |
| 23 | Gold sum(session_count) == session target | PASS | 652740 | 652740 |

## PostgreSQL publish

| # | Check | Result | Actual | Expected |
|---:|---|---|---:|---:|
| 24 | published users floor | PASS | 383965 | at least 320000 |
| 25 | PostgreSQL columns == Gold columns (names, order) | PASS | 19 columns | 19 columns |
| 26 | PostgreSQL user_features rows == Gold rows | PASS | 383965 | 383965 |
| 27 | Gold rows not in PostgreSQL | PASS | 0 | 0 |
| 28 | PostgreSQL rows not in Gold | PASS | 0 | 0 |

Check 25 compared these 19 column names, in this order, on both sides: `user_id`, `user_name`, `membership_tier`, `total_events`, `page_view_count`, `product_view_count`, `add_to_cart_count`, `purchase_count`, `search_count`, `avg_event_price`, `max_event_price`, `avg_engagement_score`, `avg_purchase_probability`, `last_event_timestamp`, `unique_products_interacted`, `total_spend`, `session_count`, `features_computed_at`, `recency_hours`.

Checks 27 and 28 compare whole rows on all 19 columns, in both directions, after casting the PostgreSQL columns to Gold's types. With both at 0 and check 26 equal, PostgreSQL `user_features` and Gold are identical on every column of every row.

## Export consistency

| # | Check | Result | Actual | Expected |
|---:|---|---|---:|---:|
| 29 | distinct event profiles == active users | PASS | 383965 | 383965 |
| 30 | event profiles not in the export | PASS | 0 | 0 |
| 31 | published name or tier not in the export | PASS | 0 | 0 |
