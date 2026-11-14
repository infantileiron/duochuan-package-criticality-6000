"""Incremental batch merge over a (source, kind, stable_id) revision ledger.

Pure stdlib logic; src/services/batches.py loads the ledger, calls plan_batch,
validates the synthesized final graph, then persists in one transaction.

Merge rules:
- records merge per (source, kind, stable_id); revision orders updates
- older revisions are ignored; same revision + same content is not rewritten
- same revision + different content is a conflict and rejects the whole batch
- a revoke writes a tombstone: re-sent revisions <= tombstone never resurrect
- within one batch only the highest revision per key takes effect and the
  final graph is synthesized before validation, so row order never matters
"""
import hashlib,json

def content_hash(content):
 return hashlib.sha256(json.dumps(content,sort_keys=True,ensure_ascii=False).encode('utf-8')).hexdigest()

def edge_stable_id(predecessor,successor):
 return predecessor+'->'+successor

def _tombstone(revision):
 return {'revision':revision,'status':'revoked','content':None,'content_hash':content_hash(None)}

def _active(revision,content):
 return {'revision':revision,'status':'active','content':content,'content_hash':content_hash(content)}

def _result(record,result,detail):
 return {'kind':record['kind'],'stable_id':record['stable_id'],'source':record['source'],'revision':record['revision'],'action':record['action'],'result':result,'detail':detail}

def _merge_one(entry,record,present):
 """Merge one winning record against its ledger entry; return (result, detail, new entry or None).
 present marks whether the record's target currently exists in the materialized graph."""
 r=record['revision']
 if entry is None:
  if record['action']=='revoke':
   if present:return 'revoked','原清单内容已撤销，旧补发不再复活',_tombstone(r)
   return 'revoked','记录尚未出现，已登记撤销标记，旧补发不再复活',_tombstone(r)
  if present:return 'applied',f'首次补发已入账，覆盖原清单内容（修订 {r}）',_active(r,record['content'])
  return 'applied','新记录已写入',_active(r,record['content'])
 current=entry['revision']
 if r<current:return 'stale',f'旧修订，已忽略（当前修订 {current}）',None
 if r==current:
  if record['action']=='revoke':
   if entry['status']=='revoked':return 'duplicate','同号撤销已入账，无需重复写入',None
   return 'revoked',f'修订 {r} 已撤销',_tombstone(r)
  if entry['status']=='revoked':return 'stale',f'该记录已于修订 {current} 撤销，旧补发不复活',None
  if entry['content_hash']==content_hash(record['content']):return 'duplicate','同号同内容，无需重复写入',None
  return 'conflict',f'同号异内容冲突：修订 {r} 与已入账内容不一致',None
 if record['action']=='revoke':return 'revoked',f'修订 {r} 已撤销（覆盖修订 {current}）',_tombstone(r)
 if entry['status']=='revoked':return 'applied',f'修订 {r} 重新启用（此前于修订 {current} 撤销）',_active(r,record['content'])
 return 'applied',f'修订 {r} 已覆盖修订 {current}',_active(r,record['content'])

def plan_batch(ledger,records,current_tasks=(),current_edges=()):
 """ledger maps (source, kind, stable_id) to its current entry; records are
 normalized dicts (kind/source/stable_id/revision/action/content, dependencies
 also carry endpoints). current_tasks/current_edges are the materialized graph
 used only for accurate result wording. Returns (results, changes, graph_ops, error)."""
 results=[None]*len(records);problems=[]
 groups={}
 for i,record in enumerate(records):groups.setdefault((record['source'],record['kind'],record['stable_id']),[]).append(i)
 winners=[]
 for (source,kind,stable_id),idxs in groups.items():
  top=max(records[i]['revision'] for i in idxs)
  best=[i for i in idxs if records[i]['revision']==top]
  for i in idxs:
   if records[i]['revision']<top:results[i]=_result(records[i],'superseded',f'同批存在更高修订 {top}，本条不生效')
  if len({(records[i]['action'],content_hash(records[i]['content'])) for i in best})>1:
   for i in best:results[i]=_result(records[i],'conflict',f'同批同号异内容冲突：修订 {top} 内容不一致')
   problems.append(f'{kind} {stable_id} 同批同号异内容冲突')
   continue
  for i in best[1:]:results[i]=_result(records[i],'duplicate','同批同号同内容，仅入账一次')
  winners.append(best[0])
 changes={};writes=[]
 task_ids=set(current_tasks);edge_set={tuple(e) for e in current_edges}
 for i in winners:
  record=records[i];key=(record['source'],record['kind'],record['stable_id'])
  present=record['stable_id'] in task_ids if record['kind']=='task' else tuple(record['endpoints']) in edge_set
  result,detail,entry=_merge_one(ledger.get(key),record,present)
  results[i]=_result(record,result,detail)
  if result=='conflict':problems.append(f"{record['kind']} {record['stable_id']} {detail}")
  if entry is not None:changes[key]=entry;writes.append((key,record,entry))
 task_upserts={};task_removes=set();edge_adds=set();edge_removes=set()
 for (source,kind,stable_id),record,entry in writes:
  if kind=='task':
   if entry['status']=='active':
    previous=task_upserts.get(stable_id)
    if previous is not None and content_hash(previous)!=content_hash(entry['content']):problems.append(f'任务 {stable_id} 同批来自不同来源且内容不一致')
    task_upserts[stable_id]=entry['content']
   else:task_removes.add(stable_id)
  else:
   edge=tuple(record['endpoints'])
   if entry['status']=='active':edge_adds.add(edge)
   else:edge_removes.add(edge)
 for task_id in sorted(task_removes&set(task_upserts)):problems.append(f'任务 {task_id} 同批既更新又撤销')
 for edge in sorted(edge_adds&edge_removes):problems.append(f'依赖 {edge[0]}->{edge[1]} 同批既添加又撤销')
 graph_ops=[('upsert_task',task_upserts[k]) for k in sorted(task_upserts)]
 graph_ops+=[('remove_task',k) for k in sorted(task_removes-set(task_upserts))]
 graph_ops+=[('add_edge',e) for e in sorted(edge_adds-edge_removes)]
 graph_ops+=[('remove_edge',e) for e in sorted(edge_removes-edge_adds)]
 return results,changes,graph_ops,'；'.join(problems)

def apply_graph_ops(tasks,edges,graph_ops):
 """Synthesize the final graph from the current graph plus batch ops."""
 table={t['task_id']:dict(t) for t in tasks};edge_set={tuple(e) for e in edges}
 for op in graph_ops:
  if op[0]=='upsert_task':table[op[1]['task_id']]=dict(op[1])
  elif op[0]=='remove_task':table.pop(op[1],None)
  elif op[0]=='add_edge':edge_set.add(tuple(op[1]))
  else:edge_set.discard(tuple(op[1]))
 return [table[k] for k in sorted(table)],sorted(edge_set)
