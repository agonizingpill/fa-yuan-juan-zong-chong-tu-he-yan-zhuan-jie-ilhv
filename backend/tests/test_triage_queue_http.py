"""HTTP 层冒烟：登录角色、疑难队列端点、workspace 快照、幂等确认。"""
import os
os.environ['DATABASE_URL']='sqlite:////tmp/triage-queue-http.db'
os.environ['SEED_DEMO']='false'

import pytest
from fastapi.testclient import TestClient

from src.app import app
from src.database import engine, Session
from src.models import Base, User, Intake, Consent, Triage
from src.auth import password_hash
from src import triage_service as ts


def setup_function():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session() as db:
        for name, role in [('supervisor', 'supervisor'), ('operator', 'operator'), ('viewer', 'viewer')]:
            db.add(User(username=name, password=password_hash('Aid@2026'), role=role))
        case = Intake(code='LZ200', alias='疑难访客', facts='公司没有发工资，租赁房屋到期房东不退押金', status='open')
        db.add(case); db.flush()
        db.add(Consent(intake_id=case.id, authorized=True, basis='模拟授权存根'))
        row = Triage(intake_id=case.id, facts_version=1, predicted='housing', confidence=.70, second='labor', margin=.42)
        db.add(row); db.flush(); ts.route(db, row)
        db.commit()


def login(client, username):
    r = client.post('/auth/login', json={'username': username, 'password': 'Aid@2026'})
    assert r.status_code == 200, r.text
    return {'Authorization': 'Bearer ' + r.json()['token']}


def test_queue_claim_confirm_flow_over_http():
    with TestClient(app) as client:
        sup = login(client, 'supervisor')
        ws = client.get('/workspace', headers=sup).json()
        assert 'triage_claims' in ws and ws['triages'][0]['status'] == 'difficult'
        assert ws['triages'][0]['queue_reason']

        denied = client.post('/triage-claims', headers=login(client, 'operator'))
        assert denied.status_code == 403
        denied = client.post('/triage-claims', headers=login(client, 'viewer'))
        assert denied.status_code == 403

        ticket = client.post('/triage-claims', headers=sup)
        assert ticket.status_code == 200, ticket.text
        tid = ticket.json()['id']
        again = client.post('/triage-claims', headers=sup)
        assert again.status_code == 409

        ok = client.post(f'/triage-claims/{tid}/confirm', json={'topic': 'housing', 'note': '已核对前两名得分'}, headers=sup)
        assert ok.status_code == 200 and ok.json()['status'] == 'reviewed'
        retry = client.post(f'/triage-claims/{tid}/confirm', json={'topic': 'housing', 'note': '重试同一结论'}, headers=sup)
        assert retry.status_code == 200 and retry.json() == ok.json()
        conflict = client.post(f'/triage-claims/{tid}/confirm', json={'topic': 'labor', 'note': '试图改结论'}, headers=sup)
        assert conflict.status_code == 409

        ws = client.get('/workspace', headers=sup).json()
        assert ws['triage_claims'][0]['status'] == 'confirmed'
        assert ws['triages'][0]['status'] == 'reviewed'
