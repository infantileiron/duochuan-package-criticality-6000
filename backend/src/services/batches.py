import uuid
from fastapi import HTTPException
from sqlalchemy import select
from src.models import WorkPackage,TaskDependency,ImportBatch,ImportRecord
from src.services.batch_merge import plan_batch,apply_graph_ops,edge_stable_id
from src.services.validation import validate_package
from src.utils.serialization import serialize
from src.utils.time import now

def _record_id(package_id,source,kind,stable_id):
 return str(uuid.uuid5(uuid.NAMESPACE_URL,f'{package_id}|{source}|{kind}|{stable_id}'))

def _edge_row_id(package_id,predecessor,successor):
 return str(uuid.uuid5(uuid.NAMESPACE_URL,f'{package_id}|edge|{predecessor}|{successor}'))

def create(db,payload,actor):
 package=db.scalar(select(WorkPackage).where(WorkPackage.id==payload.package_id).with_for_update())
 if not package:raise HTTPException(404,'工作包不存在')
 records=[]
 for t in payload.tasks:
  content=None if t.action=='revoke' else {'task_id':t.task_id,'name':t.name,'a':t.a,'m':t.m,'b':t.b}
  records.append({'kind':'task','source':t.source,'stable_id':t.task_id,'revision':t.revision,'action':t.action,'content':content})
 for d in payload.dependencies:
  content=None if d.action=='revoke' else {'predecessor':d.predecessor,'successor':d.successor}
  records.append({'kind':'dependency','source':d.source,'stable_id':edge_stable_id(d.predecessor,d.successor),'revision':d.revision,'action':d.action,'content':content,'endpoints':(d.predecessor,d.successor)})
 ledger={};rows_by_key={}
 for row in db.scalars(select(ImportRecord).where(ImportRecord.package_id==package.id)):
  ledger[(row.source,row.kind,row.stable_id)]={'revision':row.revision,'status':row.status,'content':row.content,'content_hash':row.content_hash}
  rows_by_key[(row.source,row.kind,row.stable_id)]=row
 current_edges=[(e.predecessor,e.successor) for e in db.scalars(select(TaskDependency).where(TaskDependency.package_id==package.id))]
 results,changes,graph_ops,error=plan_batch(ledger,records,current_tasks=[t['task_id'] for t in package.tasks],current_edges=current_edges)
 final_tasks=final_edges=None
 if not error:
  final_tasks,final_edges=apply_graph_ops(package.tasks,current_edges,graph_ops)
  try:validate_package(final_tasks,final_edges)
  except ValueError as e:error=str(e)
 summary={'total':len(results)}
 for r in results:summary[r['result']]=summary.get(r['result'],0)+1
 batch=ImportBatch(id=str(uuid.uuid4()),package_id=package.id,note=payload.note,actor=actor,status='rejected' if error else 'applied',error=error,summary=summary,results=results,created_at=now())
 db.add(batch)
 if not error:
  for (source,kind,stable_id),entry in changes.items():
   row=rows_by_key.get((source,kind,stable_id))
   if row:
    row.revision=entry['revision'];row.content=entry['content'];row.content_hash=entry['content_hash'];row.status=entry['status'];row.batch_id=batch.id;row.updated_at=batch.created_at
   else:db.add(ImportRecord(id=_record_id(package.id,source,kind,stable_id),package_id=package.id,source=source,kind=kind,stable_id=stable_id,revision=entry['revision'],content=entry['content'],content_hash=entry['content_hash'],status=entry['status'],batch_id=batch.id,updated_at=batch.created_at))
  package.tasks=final_tasks
  existing=list(db.scalars(select(TaskDependency).where(TaskDependency.package_id==package.id)))
  target={tuple(e) for e in final_edges}
  for row in existing:
   if (row.predecessor,row.successor) not in target:db.delete(row)
  kept={(row.predecessor,row.successor) for row in existing}
  for predecessor,successor in sorted(target-kept):
   db.add(TaskDependency(id=_edge_row_id(package.id,predecessor,successor),package_id=package.id,predecessor=predecessor,successor=successor,created_at=batch.created_at))
 db.commit();return serialize(batch)
