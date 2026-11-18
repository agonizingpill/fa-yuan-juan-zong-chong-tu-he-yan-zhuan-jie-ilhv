"""疑难队列：入队规则、领办并发、超时回队、过期拒绝、幂等确认、转介采信。

本地用 SQLite 验证状态机与唯一约束；行锁（FOR UPDATE）在 MySQL 部署下生效。
运行：cd backend && python -m pytest tests/ -v
"""
import threading
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from src.models import Base, Intake, Consent, Triage, TriageClaim, Party, CaseParty, Requirement, Document, Agency, Referral, Audit, now
from src import triage_service as ts
from src import referral_service as rs
from src.schemas import ReviewInput, ReferralInput

engine = create_engine('sqlite:////tmp/triage-queue-test.db', connect_args={'check_same_thread': False, 'timeout': 30})
Session = sessionmaker(engine, expire_on_commit=False)


def setup_function():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def case(db, code='LZ100', authorized=True):
    row = Intake(code=code, alias='测试访客', facts='模拟事实描述文本', status='open')
    db.add(row); db.flush()
    db.add(Consent(intake_id=row.id, authorized=authorized, basis='模拟授权存根'))
    return row


def difficult(db, intake, confidence=.70, margin=.42, predicted='housing', second='labor'):
    row = Triage(intake_id=intake.id, facts_version=intake.facts_version, predicted=predicted,
                 confidence=confidence, second=second, margin=margin)
    db.add(row); db.flush(); ts.route(db, row); db.flush()
    return row


def review_input(topic='housing'):
    return ReviewInput(topic=topic, note='已核对前两名主题得分')


SUP1 = SimpleNamespace(username='sup1')
SUP2 = SimpleNamespace(username='sup2')


def test_route_difficult_only_when_close_and_over_line():
    with Session() as db:
        c = case(db)
        row = difficult(db, c)
        assert row.status == 'difficult' and row.queue_position == 1
        assert '前两名' in row.queue_reason and row.manual_required is False
        wide = difficult(db, c, confidence=.9, margin=.8)
        assert wide.status == 'done' and wide.queue_position is None
        low = difficult(db, c, confidence=.5, margin=.01)
        assert low.status == 'done' and low.manual_required is True


def test_claim_takes_longest_waiting_first():
    with Session() as db:
        c1, c2 = case(db, 'LZ101'), case(db, 'LZ102')
        t1, t2 = difficult(db, c1), difficult(db, c2)
        first = ts.claim(db, SUP1)
        assert first['triage_id'] == t1.id and first['position'] == t1.queue_position
        assert first['claimed_by'] == 'sup1' and first['status'] == 'open'
        second = ts.claim(db, SUP2)
        assert second['triage_id'] == t2.id
        assert db.get(Triage, t1.id).status == 'claimed'
        with pytest.raises(HTTPException) as e:
            ts.claim(db, SUP1)
        assert e.value.status_code == 409


def test_concurrent_claims_never_share_a_case():
    with Session() as db:
        c1, c2 = case(db, 'LZ103'), case(db, 'LZ104')
        difficult(db, c1); difficult(db, c2)
        db.commit()
    barrier, results = threading.Barrier(6), []

    def work(user):
        barrier.wait()
        with Session() as db:
            try:
                results.append(('ok', ts.claim(db, user)['triage_id']))
            except (HTTPException, IntegrityError):
                results.append(('rejected', None))
    users = [SimpleNamespace(username=f'sup{i}') for i in range(6)]
    threads = [threading.Thread(target=work, args=(u,)) for u in users]
    [t.start() for t in threads]; [t.join() for t in threads]
    won = [tid for status, tid in results if status == 'ok']
    assert len(won) == len(set(won)) != 0, f'同一卷被两人领取: {results}'
    assert len(won) <= 2


def test_claim_skips_unauthorized_and_stale_version():
    with Session() as db:
        noauth = case(db, 'LZ105', authorized=False)
        difficult(db, noauth)
        stale = case(db, 'LZ106')
        old = difficult(db, stale)
        stale.facts_version += 1
        fresh = case(db, 'LZ107')
        t3 = difficult(db, fresh)
        ticket = ts.claim(db, SUP1)
        assert ticket['triage_id'] == t3.id


def test_confirm_then_referral_accepts_conclusion():
    with Session() as db:
        c = case(db)
        for side, name in [('client', '模拟来访甲'), ('opponent', '模拟相对乙')]:
            p = Party(name=name, normalized=name); db.add(p); db.flush()
            db.add(CaseParty(intake_id=c.id, party_id=p.id, side=side))
        agency = Agency(code='TS', name='测试协作点', specialties='housing,labor', capacity=2, active_count=0)
        db.add(agency); db.flush()
        for code in ['identity', 'evidence']:
            req = Requirement(topic='housing', code=code, title=f'住房材料{code}')
            db.add(req); db.flush()
            db.add(Document(intake_id=c.id, requirement_id=req.id, reference='脱敏索引.pdf', status='verified'))
        row = difficult(db, c)
        ticket = ts.claim(db, SUP1)
        done = ts.confirm(db, ticket['id'], review_input('housing'), SUP1)
        assert done['status'] == 'reviewed' and done['approved'] == 'housing'
        assert db.get(TriageClaim, ticket['id']).status == 'confirmed'
        assert ts.latest(db, c.id).approved == 'housing'
        ref = rs.refer(db, ReferralInput(intake_id=c.id, agency_id=agency.id), SUP1)
        assert ref['topic'] == 'housing' and ref['status'] == 'pending'
        assert db.get(Agency, agency.id).active_count == 1


def test_confirm_retry_is_idempotent_and_audited_once():
    with Session() as db:
        c = case(db)
        difficult(db, c)
        ticket = ts.claim(db, SUP1)
        first = ts.confirm(db, ticket['id'], review_input('housing'), SUP1)
        audits = db.scalar(select(func.count(Audit.id)))
        again = ts.confirm(db, ticket['id'], review_input('housing'), SUP1)
        assert again == first
        assert db.scalar(select(func.count(Audit.id))) == audits
        assert db.scalar(select(func.count(TriageClaim.id))) == 1
        with pytest.raises(HTTPException) as e:
            ts.confirm(db, ticket['id'], review_input('labor'), SUP1)
        assert e.value.status_code == 409


def test_expired_ticket_rejected_and_case_returns_to_original_position():
    with Session() as db:
        c1, c2 = case(db, 'LZ108'), case(db, 'LZ109')
        t1, t2 = difficult(db, c1), difficult(db, c2)
        ticket = ts.claim(db, SUP1)
        db.get(TriageClaim, ticket['id']).expires_at = now() - __import__('datetime').timedelta(seconds=1)
        db.commit()
        with pytest.raises(HTTPException) as e:
            ts.confirm(db, ticket['id'], review_input(), SUP1)
        assert e.value.status_code == 409 and '过期' in e.value.detail
        row = db.get(Triage, t1.id)
        assert row.status == 'difficult' and row.queue_position == t1.queue_position
        assert db.get(TriageClaim, ticket['id']).status == 'expired'
        again = ts.claim(db, SUP2)
        assert again['triage_id'] == t1.id and again['position'] == t1.queue_position


def test_expire_stale_sweeps_open_tickets():
    with Session() as db:
        c = case(db)
        row = difficult(db, c)
        ticket = ts.claim(db, SUP1)
        db.get(TriageClaim, ticket['id']).expires_at = now() - __import__('datetime').timedelta(seconds=1)
        db.commit()
        stale = ts.expire_stale(db); db.commit()
        assert len(stale) == 1
        assert db.get(Triage, row.id).status == 'difficult'
        assert db.get(TriageClaim, ticket['id']).open_key is None


def test_confirm_rejects_other_supervisor_and_normal_review_skips_difficult():
    with Session() as db:
        c = case(db)
        row = difficult(db, c)
        ticket = ts.claim(db, SUP1)
        with pytest.raises(HTTPException) as e:
            ts.confirm(db, ticket['id'], review_input(), SUP2)
        assert e.value.status_code == 409
        with pytest.raises(HTTPException) as e:
            ts.review(db, row.id, review_input(), SUP1)
        assert e.value.status_code == 409
