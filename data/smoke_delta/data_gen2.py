import csv, os
os.makedirs("data/landing", exist_ok=True)

new_claims = [
    {"claim_id": 41, "policy_id": 5,  "claim_date": "2024-11-02", "claim_type": "Collision",    "amount": 3200.50,  "status": "Submitted",  "updated_at": "2024-11-02 10:00:00"},
    {"claim_id": 42, "policy_id": 12, "claim_date": "2024-11-03", "claim_type": "Theft",        "amount": 7800.00,  "status": "approved",   "updated_at": "2024-11-03 10:00:00"},
    {"claim_id": 43, "policy_id": 7,  "claim_date": "2024-11-05", "claim_type": "Medical",      "amount": 1250.75,  "status": " Approved ", "updated_at": "2024-11-05 10:00:00"},
    {"claim_id": 44, "policy_id": 18, "claim_date": "2024-11-08", "claim_type": "Fire",         "amount": 15000.00, "status": "paid",       "updated_at": "2024-11-08 10:00:00"},
    {"claim_id": 45, "policy_id": 3,  "claim_date": "2024-11-10", "claim_type": "Water Damage", "amount": 940.20,   "status": "Denied",     "updated_at": "2024-11-10 10:00:00"},
    {"claim_id": 46, "policy_id": 9,  "claim_date": "2024-11-11", "claim_type": "Collision",    "amount": "",        "status": "Submitted",  "updated_at": "2024-11-11 10:00:00"},
]  # claim 46 is dirty on purpose: no amount -> quarantine should catch it again
with open("data/landing/claims_new.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(new_claims[0].keys())); w.writeheader(); w.writerows(new_claims)
print("claims_new:", len(new_claims))

updates = [
    {"claim_id": 2,  "new_status": "PAID",     "updated_at": "2024-11-15 09:00:00"},
    {"claim_id": 6,  "new_status": "APPROVED", "updated_at": "2024-11-15 09:00:00"},
    {"claim_id": 10, "new_status": "APPROVED", "updated_at": "2024-11-15 09:00:00"},
]
with open("data/landing/status_updates.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(updates[0].keys())); w.writeheader(); w.writerows(updates)
print("status_updates:", len(updates))
