"""Temporary-DB tests for versioned payer-rule persistence."""
import importlib
import os
import sys
import tempfile
from pathlib import Path


def test_rule_round_trip_and_deactivation():
    with tempfile.TemporaryDirectory(prefix="elig-rule-") as tmp:
        root = Path(tmp)
        os.environ["DB_PATH"] = str(root / "hub.db")
        os.environ["CLIENTS_SEED_PATH"] = str(root / "clients_seed.json")
        (root / "clients_seed.json").write_text("[]\n", encoding="utf-8")
        if "app.config" in sys.modules:
            importlib.reload(sys.modules["app.config"])
        import app.client_db as client_db
        client_db = importlib.reload(client_db)
        client_db.init_client_hub_db()

        rule_id = client_db.save_eligibility_payer_rule({
            "rule_key": "aetna-87631-pa",
            "payer_pattern": "aetna",
            "plan_pattern": "ppo",
            "cpt_code": "87631",
            "criteria": {"icd10_any": ["J12.81"]},
            "decision": "PA_REQUIRED",
            "reason": "Current policy requires PA.",
            "actions": ["Submit prior authorization"],
            "source": "Aetna policy bulletin 123",
            "version": "2026-07",
        }, updated_by="admin")
        rules = client_db.list_eligibility_payer_rules()
        assert rules[0]["id"] == rule_id
        assert rules[0]["criteria"] == {"icd10_any": ["J12.81"]}
        assert rules[0]["actions"] == ["Submit prior authorization"]

        client_db.save_eligibility_payer_rule({**rules[0], "reason": "Updated."}, "admin")
        assert client_db.list_eligibility_payer_rules()[0]["reason"] == "Updated."
        assert client_db.deactivate_eligibility_payer_rule(rule_id, "admin")
        assert client_db.list_eligibility_payer_rules() == []
        assert client_db.list_eligibility_payer_rules(include_inactive=True)[0]["is_active"] is False


if __name__ == "__main__":
    test_rule_round_trip_and_deactivation()
    print("eligibility payer-rule persistence test passed")