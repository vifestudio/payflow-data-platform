from fastapi import FastAPI, Query
from datetime import date, timedelta
import numpy as np
import uvicorn

app = FastAPI(title="PayFlow FX Rates API")

BASE_RATES = {
    "EUR": 0.9234,
    "GBP": 0.7891,
    "CRC": 519.45,
    "MXN": 17.23,
    "BRL": 5.67,
    "CAD": 1.36,
}


def rate_for_date(currency: str, d: date) -> float:
    seed = int(d.strftime("%Y%m%d")) + abs(hash(currency)) % 10000
    rng = np.random.default_rng(seed)
    variation = rng.uniform(-0.015, 0.015)
    return round(BASE_RATES[currency] * (1 + variation), 6)


@app.get("/rates")
def get_rates(on: date = Query(default=None)):
    target = on or date.today()
    return [
        {"date": str(target), "base": "USD", "currency": c, "rate": rate_for_date(c, target)}
        for c in BASE_RATES
    ]


@app.get("/rates/history")
def get_history(days: int = Query(default=30, ge=1, le=365)):
    today = date.today()
    records = []
    for i in range(days):
        d = today - timedelta(days=i)
        for c in BASE_RATES:
            records.append({"date": str(d), "base": "USD", "currency": c, "rate": rate_for_date(c, d)})
    return records


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
