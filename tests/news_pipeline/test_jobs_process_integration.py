from argparse import Namespace
import json

from news_pipeline import event_store, jobs, process_runner


def test_process_job_feeds_phase4_event_store(monkeypatch, capsys):
    calls = []

    class Phase3Report:
        pass

    class Phase4Report:
        pass

    def fake_phase3(db, evaluated_at, *, history_db_path, max_items):
        calls.append(("phase3", db, evaluated_at, history_db_path, max_items))
        return Phase3Report()

    def fake_phase4(db, evaluated_at, *, max_items):
        calls.append(("phase4", db, evaluated_at, max_items))
        return Phase4Report()

    monkeypatch.setattr(process_runner, "process_news", fake_phase3)
    monkeypatch.setattr(event_store, "process_phase4", fake_phase4)
    args = Namespace(
        enable_network=True,
        db="/tmp/news-state.db",
        evaluated_at="2026-09-14T20:00:00Z",
        history_db=None,
        max_items=17,
    )

    assert jobs._run_process(args) == 0
    assert calls == [
        ("phase3", "/tmp/news-state.db", "2026-09-14T20:00:00Z", None, 17),
        ("phase4", "/tmp/news-state.db", "2026-09-14T20:00:00Z", 17),
    ]
    output = json.loads(capsys.readouterr().out)
    assert output["job"] == "process"
    assert output["state"] == "completed"
    assert output["network_used"] is False
    assert "event_report" in output
