"""Generate a synthetic financial-transactions CSV for the Spark RDD project.

    python data/generate_transactions.py [--rows 5000] [--seed 42] [--out data/transactions.csv]

The data is random but *structured* so the analyses have something to find:
  * every vendor belongs to one category, and amounts follow a per-category distribution;
  * each user has a home city (most transactions happen there) and 1-2 bank accounts,
    each account held at one financial institution;
  * spending is seasonal (more in December and summer, travel peaks in July/August).
No field contains a comma, so the RDD code can split lines with a plain `split(",")`.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
from datetime import datetime, timedelta
from pathlib import Path

HEADER = [
    "user_id",
    "transaction_id",
    "bank_account_id",
    "financial_institution_id",
    "date",
    "third_party",
    "category",
    "transaction_amount",
    "city",
    "payment_method",
    "currency",
]

# category -> (vendors, median amount EUR, spread)
CATEGORIES: dict[str, tuple[list[str], float, float]] = {
    "Food": (["Carrefour", "Leclerc", "Auchan", "Lidl", "Monoprix", "Picard"], 38.0, 0.6),
    "Restaurants": (["McDonalds", "Starbucks", "Deliveroo", "UberEats", "Paul"], 22.0, 0.6),
    "Travel": (["SNCF", "Air France", "Booking.com", "Airbnb", "BlaBlaCar"], 180.0, 0.8),
    "Shopping": (["Amazon", "Fnac", "Zara", "Decathlon", "IKEA", "H&M"], 65.0, 0.8),
    "Health": (["Pharmacie Centrale", "Doctolib", "Optic 2000", "Clinique Pasteur"], 45.0, 0.7),
    "Entertainment": (["Netflix", "Spotify", "UGC Cinemas", "Steam", "Ticketmaster"], 18.0, 0.7),
    "Utilities": (["EDF", "Engie", "Orange", "SFR", "Veolia"], 75.0, 0.4),
    "Transport": (["TotalEnergies", "RATP", "Uber", "Lime", "Shell"], 32.0, 0.6),
    "Education": (["Udemy", "Coursera", "Cultura", "Gibert Joseph"], 55.0, 0.7),
}

# Relative weight of each category in the number of transactions.
CATEGORY_WEIGHTS = {
    "Food": 24,
    "Restaurants": 16,
    "Shopping": 14,
    "Transport": 13,
    "Entertainment": 9,
    "Utilities": 8,
    "Health": 7,
    "Travel": 5,
    "Education": 4,
}

CITIES = {  # city -> population weight
    "Paris": 30,
    "Lyon": 12,
    "Marseille": 11,
    "Toulouse": 9,
    "Nice": 7,
    "Nantes": 7,
    "Lille": 7,
    "Bordeaux": 7,
    "Strasbourg": 5,
    "Rouen": 5,
}

INSTITUTIONS = {  # financial_institution_id -> market-share weight
    101: 22,  # e.g. BNP Paribas
    102: 18,  # Credit Agricole
    103: 15,  # Societe Generale
    104: 12,  # LCL
    105: 10,  # Caisse d'Epargne
    106: 8,  # Banque Populaire
    107: 6,  # La Banque Postale
    108: 5,  # Boursorama
    109: 4,  # Revolut
}

# Seasonality multiplier for the number of transactions per month (Jan..Dec).
MONTH_WEIGHTS = [0.85, 0.8, 0.9, 0.95, 1.0, 1.05, 1.2, 1.15, 0.95, 0.95, 1.05, 1.4]
PAYMENT_METHODS = {"Card": 62, "Contactless": 18, "Transfer": 10, "Direct Debit": 7, "Mobile": 3}


def weighted(rng: random.Random, table: dict) -> object:
    keys = list(table)
    return rng.choices(keys, weights=[table[k] for k in keys], k=1)[0]


def generate(rows: int, seed: int, year: int = 2025, users: int = 250) -> list[list[str]]:
    rng = random.Random(seed)

    # Users: a home city and 1-2 accounts, each at one institution.
    profiles = []
    for u in range(1, users + 1):
        accounts = []
        for _ in range(1 if rng.random() < 0.7 else 2):
            accounts.append((f"ACC{rng.randint(10_000_000, 99_999_999)}", weighted(rng, INSTITUTIONS)))
        activity = rng.lognormvariate(0, 0.6)  # some users transact far more than others
        profiles.append((f"U{u:04d}", weighted(rng, CITIES), accounts, activity))
    user_weights = [p[3] for p in profiles]

    start = datetime(year, 1, 1)
    days_in_year = (datetime(year + 1, 1, 1) - start).days
    day_weights = [MONTH_WEIGHTS[(start + timedelta(days=d)).month - 1] for d in range(days_in_year)]

    records = []
    for _ in range(rows):
        user_id, home, accounts, _ = rng.choices(profiles, weights=user_weights, k=1)[0]
        account, institution = rng.choice(accounts)
        category = weighted(rng, CATEGORY_WEIGHTS)
        day = rng.choices(range(days_in_year), weights=day_weights, k=1)[0]
        when = start + timedelta(
            days=day, hours=rng.randint(7, 22), minutes=rng.randint(0, 59), seconds=rng.randint(0, 59)
        )
        if category == "Travel" and when.month not in (7, 8, 12) and rng.random() < 0.5:
            category = "Shopping"  # thin out travel outside holiday months
        vendors, median, spread = CATEGORIES[category]
        amount = rng.lognormvariate(math.log(median), spread)
        if when.month == 12 and category in ("Shopping", "Entertainment"):
            amount *= 1.35  # holiday spending
        city = home if rng.random() < 0.8 else weighted(rng, CITIES)
        records.append(
            [
                user_id,
                "",  # transaction_id, assigned after sorting
                account,
                str(institution),
                when.strftime("%Y-%m-%d %H:%M:%S"),
                rng.choice(vendors),
                category,
                f"{max(amount, 1.0):.2f}",
                city,
                weighted(rng, PAYMENT_METHODS),
                "EUR",
            ]
        )
    records.sort(key=lambda r: r[4])  # chronological, like a bank export
    for i, record in enumerate(records, start=1):
        record[1] = f"TXN{i:07d}"  # IDs follow time order
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=Path(__file__).with_name("transactions.csv"))
    args = parser.parse_args()
    if args.rows < 1000:
        parser.error("the project requires at least 1000 records")

    rows = generate(args.rows, args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(HEADER)
        writer.writerows(rows)
    print(f"Wrote {len(rows)} transactions to {args.out} ({args.out.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
