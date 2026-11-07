"""
Generate transform/payflow/seeds/mcc_catalog.csv from data/mcc_codes.json.

Assigns a risk_tier (very_high/high/medium/low) and mcc_category to each of the
~980 MCC codes using range-based industry heuristics derived from Visa/MC fraud
data and chargeback rates by category.
"""
import json
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC  = ROOT / "data" / "mcc_codes.json"
DEST = ROOT / "transform" / "payflow" / "seeds" / "mcc_catalog.csv"

# ---------------------------------------------------------------------------
# Category + risk_tier by MCC range.
# Source: Visa/MC published chargeback rates, Nilson Report CNP fraud by MCC.
# ---------------------------------------------------------------------------
def classify(mcc: int) -> tuple[str, str]:
    # Gambling — highest chargeback rates in the industry
    if mcc in (7801, 7802, 7995):
        return "gambling", "very_high"
    # Airlines — high value, CNP, frequent chargebacks
    if 3000 <= mcc <= 3299:
        return "airlines", "high"
    # Hotels / Lodging
    if 3300 <= mcc <= 3499 or mcc == 7011:
        return "lodging", "high"
    # Car rental
    if 3500 <= mcc <= 3999 or mcc == 7512:
        return "car_rental", "high"
    # Digital goods (modern MCCs added by Visa/MC ~2012)
    if mcc in (5815, 5816, 5817, 5818):
        return "digital_goods", "high"
    # Computer / software stores — CNP, no physical verification
    if mcc in (5045, 5734, 7372, 7371, 7374, 7375, 7379):
        return "software_tech", "high"
    # Direct marketing — historically highest CNP fraud
    if 5960 <= mcc <= 5969:
        return "direct_marketing", "high"
    # Financial institutions / quasi-cash
    if 6000 <= mcc <= 6099:
        return "financial_cash", "high"
    # Money transfer / wire
    if mcc in (4829, 6540):
        return "money_transfer", "high"
    # Travel agencies, tour operators
    if mcc in (4722, 4723, 7011, 7012, 7399):
        return "travel", "high"
    # Taxicabs / rideshare (GPS-verified, lower risk than pure CNP)
    if mcc in (4111, 4121, 4131):
        return "transportation", "low"
    # Airlines (domestic commuter) + transit
    if 4000 <= mcc <= 4299:
        return "transportation", "low"
    # Utilities
    if 4300 <= mcc <= 4999:
        return "utilities", "low"
    # Wholesale trade
    if 5000 <= mcc <= 5099:
        return "wholesale", "medium"
    # Petroleum / fuel wholesale
    if 5100 <= mcc <= 5199:
        return "petroleum_wholesale", "medium"
    # Home improvement / building materials
    if 5200 <= mcc <= 5299:
        return "home_improvement", "medium"
    # Discount / variety stores (Walmart, Target)
    if 5300 <= mcc <= 5399:
        return "discount_retail", "medium"
    # Grocery / supermarkets
    if 5400 <= mcc <= 5499:
        return "grocery", "low"
    # Automotive
    if 5500 <= mcc <= 5599:
        return "automotive", "low"
    # Clothing / apparel
    if 5600 <= mcc <= 5699:
        return "apparel", "medium"
    # Furniture / home
    if 5700 <= mcc <= 5799:
        return "furniture", "low"
    # Eating places / restaurants
    if 5800 <= mcc <= 5814:
        return "food_beverage", "low"
    # Service stations / gas
    if mcc == 5541 or mcc == 5542:
        return "fuel", "low"
    # Drug stores / pharmacies
    if mcc in (5912, 5122):
        return "pharmacy", "low"
    # Misc retail / specialty
    if 5900 <= mcc <= 5999:
        return "misc_retail", "medium"
    # Finance / credit / insurance
    if 6100 <= mcc <= 6399:
        return "finance_insurance", "medium"
    # Securities / brokerages
    if 6400 <= mcc <= 6499:
        return "securities", "high"
    # Personal services (laundry, beauty, etc.)
    if 7200 <= mcc <= 7299:
        return "personal_services", "low"
    # Business services
    if 7300 <= mcc <= 7399:
        return "business_services", "medium"
    # Auto repair
    if 7500 <= mcc <= 7599:
        return "auto_repair", "low"
    # Amusement / entertainment (non-gambling)
    if 7700 <= mcc <= 7799:
        return "entertainment", "medium"
    # Recreation / sports
    if 7900 <= mcc <= 7994:
        return "recreation", "medium"
    # Healthcare / medical
    if 8000 <= mcc <= 8099:
        return "healthcare", "low"
    # Legal / professional
    if 8100 <= mcc <= 8299:
        return "professional_services", "low"
    # Education
    if 8200 <= mcc <= 8299:
        return "education", "low"
    # Nonprofits / charities
    if 8300 <= mcc <= 8699:
        return "nonprofit", "low"
    # Government
    if 9000 <= mcc <= 9999:
        return "government", "low"
    return "misc", "medium"


with open(SRC) as f:
    codes = json.load(f)

rows = []
for entry in codes:
    mcc_int = int(entry["mcc"])
    description = entry.get("edited_description") or entry.get("combined_description") or ""
    category, risk_tier = classify(mcc_int)
    rows.append({
        "mcc_code": entry["mcc"],
        "description": description.replace(",", " "),
        "mcc_category": category,
        "risk_tier": risk_tier,
    })

rows.sort(key=lambda r: r["mcc_code"])

with open(DEST, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["mcc_code", "description", "mcc_category", "risk_tier"])
    writer.writeheader()
    writer.writerows(rows)

print(f"Written {len(rows)} MCC codes to {DEST}")
by_tier = {}
for r in rows:
    by_tier[r["risk_tier"]] = by_tier.get(r["risk_tier"], 0) + 1
for tier, n in sorted(by_tier.items()):
    print(f"  {tier}: {n}")
