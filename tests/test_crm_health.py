"""Честный пульс (09.07): heartbeat несёт здоровье CRM-канала (блок crm),
/api/health/scheduler отдаёт crm_ok — по нему iOS красит точку.
Урок 12-дневного молчаливого простоя CDP: точка была зелёной при мёртвом канале.
"""


def _hb(app_client, internal_headers, crm=None):
    client, _ = app_client
    payload = {"pid": 1, "workers": ["regen-worker"]}
    if crm is not None:
        payload["crm"] = crm
    r = client.post("/api/internal/heartbeat", json=payload, headers=internal_headers)
    assert r.status_code == 200
    return r


def _health(app_client, widget_headers):
    client, _ = app_client
    r = client.get("/api/health/scheduler", headers=widget_headers)
    assert r.status_code == 200
    return r.json()


def test_heartbeat_without_crm_block_gives_crm_ok_none(app_client, widget_headers, internal_headers):
    """Старый воркер без блока crm → crm_ok=None (неизвестно), online как раньше."""
    _hb(app_client, internal_headers)
    h = _health(app_client, widget_headers)
    assert h["online"] is True
    assert h["crm_ok"] is None


def test_crm_healthy(app_client, widget_headers, internal_headers):
    _hb(app_client, internal_headers,
        crm={"cdp_ok": True, "auth_ok": True, "lead_sync_age_sec": 90, "task_sync_age_sec": 30})
    h = _health(app_client, widget_headers)
    assert h["online"] is True
    assert h["crm_ok"] is True
    assert h["lead_sync_age_sec"] == 90


def test_crm_down_when_cdp_dead(app_client, widget_headers, internal_headers):
    _hb(app_client, internal_headers, crm={"cdp_ok": False, "lead_sync_age_sec": 30})
    h = _health(app_client, widget_headers)
    assert h["crm_ok"] is False
    assert h["crm_cdp_ok"] is False


def test_crm_down_when_logged_out(app_client, widget_headers, internal_headers):
    """Разлогин amoCRM: порт жив, но auth_ok=false → канал считается упавшим."""
    _hb(app_client, internal_headers, crm={"cdp_ok": True, "auth_ok": False, "lead_sync_age_sec": 30})
    h = _health(app_client, widget_headers)
    assert h["crm_ok"] is False
    assert h["crm_auth_ok"] is False


def test_crm_down_when_sync_stale(app_client, widget_headers, internal_headers):
    """Порт жив, логин жив, но чтение CRM не проходило > 30 мин → канал мёртв."""
    _hb(app_client, internal_headers, crm={"cdp_ok": True, "auth_ok": True, "lead_sync_age_sec": 7200})
    h = _health(app_client, widget_headers)
    assert h["crm_ok"] is False


def test_transition_down_then_up_survives(app_client, widget_headers, internal_headers):
    """Переход live→dead→live не падает (пуш при пустом реестре устройств = no-op)
    и crm_ok честно меняется."""
    _hb(app_client, internal_headers, crm={"cdp_ok": True, "lead_sync_age_sec": 10})
    assert _health(app_client, widget_headers)["crm_ok"] is True
    _hb(app_client, internal_headers, crm={"cdp_ok": False})
    assert _health(app_client, widget_headers)["crm_ok"] is False
    _hb(app_client, internal_headers, crm={"cdp_ok": True, "lead_sync_age_sec": 10})
    assert _health(app_client, widget_headers)["crm_ok"] is True
