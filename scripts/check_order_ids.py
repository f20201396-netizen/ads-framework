"""
Sample order_ids from user_transaction_history to verify mandate pattern.
"""
import asyncio
import logging
from services.shared.bq_client import BQClient

logging.basicConfig(level=logging.WARNING)

# Sample recent order_ids to verify mandate pattern
QUERY = """
SELECT order_id, amount, plan_id
FROM user_transaction_history
WHERE status = 'CHARGED'
  AND amount > 50
  AND payment_date >= '2026-01-01'
ORDER BY payment_date DESC
LIMIT 50
"""

# Count how many have 'md' in order_id
MANDATE_SAMPLES_QUERY = """
SELECT order_id, amount, plan_id
FROM user_transaction_history
WHERE status = 'CHARGED'
  AND amount > 50
  AND order_id ILIKE '%md%'
  AND payment_date >= '2026-04-10'
ORDER BY payment_date DESC
LIMIT 20
"""

MANDATE_ATTR_QUERY = """
SELECT
    uth.order_id,
    uth.amount,
    uad.network,
    uad.tracker_campaign_id
FROM user_transaction_history uth
JOIN users u ON u.id = uth.user_id
LEFT JOIN user_additional_details uad ON uad.user_id = uth.user_id
WHERE uth.status = 'CHARGED'
  AND uth.amount > 50
  AND uth.order_id ILIKE '%md%'
  AND uth.payment_date >= '2026-04-10'
  AND uad.network = 'Facebook'
LIMIT 10
"""

def main():
    client = BQClient()
    print("=== Sample mandate order_ids (Apr 10+) ===")
    rows, _ = client.stream_rows(MANDATE_SAMPLES_QUERY, label="mandate_samples")
    for r in rows:
        print(f"  order_id={str(r['order_id'])!r:45s}  amount={r['amount']}  plan={r['plan_id']}")

    print("\n=== Mandate + Facebook attributed (Apr 10+) ===")
    rows2, _ = client.stream_rows(MANDATE_ATTR_QUERY, label="mandate_attr")
    print(f"  {len(rows2)} Facebook-attributed mandate payments found")
    for r in rows2[:5]:
        print(f"  order_id={str(r['order_id'])!r:40s}  campaign={r['tracker_campaign_id']}")

main()
