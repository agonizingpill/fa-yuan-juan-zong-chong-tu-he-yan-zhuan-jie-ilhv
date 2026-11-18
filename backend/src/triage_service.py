import os,redis
from datetime import timedelta,timezone
from fastapi import HTTPException
from sqlalchemy import select,func
from .models import Intake,Triage,TriageClaim,now
from .common import get,reject,finish,editable,dump
from .intake_service import authorized
queue=redis.Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)
CONFIDENCE=.68
MARGIN=float(os.getenv('TRIAGE_MARGIN_THRESHOLD','0.5'))
CLAIM_TTL=int(os.getenv('TRIAGE_CLAIM_TTL_SECONDS','600'))
TOPIC_LABELS={'labor':'劳动报酬','housing':'住房租赁','consumer':'消费服务'}
def aware(value):return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
def latest(db,id):
    case=get(db,Intake,id)
    return db.scalar(select(Triage).where(Triage.intake_id==id,Triage.facts_version==case.facts_version,Triage.status=='reviewed').order_by(Triage.id.desc()))
def enqueue(db,data,user):
    case=editable(db,data.intake_id);reject(not authorized(db,case.id),'未取得授权，不能提交行政分流')
    reject(db.scalar(select(Triage).where(Triage.intake_id==case.id,Triage.facts_version==case.facts_version,Triage.status.in_(['queued','running']))) is not None,'当前版本仍在分流')
    row=Triage(intake_id=case.id,facts_version=case.facts_version);db.add(row);db.flush();result=finish(db,user,'提交主题分流',row)
    try:queue.rpush('aid:triage',row.id)
    except redis.RedisError:
        row.status='failed';row.error='任务队列暂不可用，恢复后可重新提交';db.commit()
        raise HTTPException(503,row.error)
    return result
def review(db,id,data,user):
    initial=get(db,Triage,id);case=get(db,Intake,initial.intake_id,True);row=get(db,Triage,id,True)
    reject(row.status!='done','等待分流完成再复核')
    reject(row.facts_version!=case.facts_version,'事实已更正，请重新分流')
    reject(not authorized(db,case.id),'来访授权已撤回')
    row.approved=data.topic;row.review_note=data.note;row.status='reviewed';return finish(db,user,'专长人工复核',row)
def route(db,row):
    # 过线但前两名分差不足，转入疑难队列；低置信仍退回普通人工复核。
    row.manual_required=row.confidence<CONFIDENCE
    if not row.manual_required and row.margin is not None and row.margin<MARGIN:
        row.status='difficult';row.queue_position=(db.scalar(select(func.coalesce(func.max(Triage.queue_position),0))) or 0)+1
        row.queue_reason=f'前两名主题得分接近：{TOPIC_LABELS.get(row.predicted,row.predicted)} {row.confidence:.0%} 已过线，其次为{TOPIC_LABELS.get(row.second,row.second)}，分差 {row.margin:.1%} 低于疑难线 {MARGIN:.0%}，须主管领办复核'
    else:row.status='done'
def expire_stale(db):
    stale=[c for c in db.scalars(select(TriageClaim).where(TriageClaim.status=='open')) if aware(c.expires_at)<=now()]
    for c in stale:
        row=db.get(Triage,c.triage_id);c.status='expired';c.open_key=None
        if row and row.status=='claimed':row.status='difficult'
    db.flush();return stale
def claim(db,user):
    expire_stale(db)
    candidates=list(db.scalars(select(Triage).where(Triage.status=='difficult').order_by(Triage.queue_position)))
    claimed={c.triage_id for c in db.scalars(select(TriageClaim).where(TriageClaim.triage_id.in_([t.id for t in candidates]),TriageClaim.status=='open'))} if candidates else set()
    for t in candidates:
        if t.id in claimed:continue
        case=get(db,Intake,t.intake_id,True)
        if case.status=='closed' or case.facts_version!=t.facts_version or not authorized(db,case.id):continue
        row=get(db,Triage,t.id,True)
        if row.status!='difficult':continue
        if db.scalar(select(TriageClaim).where(TriageClaim.triage_id==row.id,TriageClaim.status=='open')) is not None:continue
        ticket=TriageClaim(triage_id=row.id,intake_id=case.id,claimed_by=user.username,position=row.queue_position,open_key=row.id,expires_at=now()+timedelta(seconds=CLAIM_TTL))
        row.status='claimed';db.add(ticket);db.flush();return finish(db,user,'领取疑难分流卷宗',ticket)
    reject(True,'疑难队列暂无可领取卷宗')
def confirm(db,id,data,user):
    initial=get(db,TriageClaim,id);case=get(db,Intake,initial.intake_id,True);row=get(db,Triage,initial.triage_id,True);ticket=get(db,TriageClaim,id,True)
    reject(ticket.claimed_by!=user.username,'该领单由其他主管领办')
    if ticket.status=='confirmed':
        reject(row.approved!=data.topic,'该领单已确认落案，结论不可更改')
        return dump(row)
    if ticket.status=='expired' or aware(ticket.expires_at)<=now():
        if ticket.status!='expired':
            ticket.status='expired';ticket.open_key=None
            if row.status=='claimed':row.status='difficult'
            db.commit()
        reject(True,'领单已过期，卷宗已回到原排位，请重新领取')
    reject(case.status=='closed','卷宗已销案，不能修改')
    reject(row.facts_version!=case.facts_version,'事实已更正，请重新分流')
    reject(not authorized(db,case.id),'来访授权已撤回')
    row.approved=data.topic;row.review_note=data.note;row.status='reviewed';ticket.status='confirmed';ticket.open_key=None;ticket.confirmed_at=now()
    return finish(db,user,'疑难分流确认落案',row)
