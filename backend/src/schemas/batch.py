from pydantic import BaseModel,Field,ConfigDict,model_validator
from typing import Literal
class TaskRecordInput(BaseModel):
 model_config=ConfigDict(extra='forbid',allow_inf_nan=False)
 task_id:str=Field(min_length=1,max_length=24)
 source:str=Field(min_length=2,max_length=180)
 revision:int=Field(ge=0,le=1000000)
 action:Literal['upsert','revoke']
 name:str|None=Field(default=None,min_length=1,max_length=80)
 a:float|None=Field(default=None,ge=0)
 m:float|None=Field(default=None,ge=0)
 b:float|None=Field(default=None,ge=0)
 @model_validator(mode='after')
 def check_content(self):
  if self.action=='upsert' and None in (self.name,self.a,self.m,self.b):raise ValueError('任务写入记录须含name/a/m/b')
  if self.action=='revoke' and (self.name,self.a,self.m,self.b)!=(None,None,None,None):raise ValueError('任务撤销记录不携带工期内容')
  return self
class DependencyRecordInput(BaseModel):
 model_config=ConfigDict(extra='forbid',allow_inf_nan=False)
 predecessor:str=Field(min_length=1,max_length=24)
 successor:str=Field(min_length=1,max_length=24)
 source:str=Field(min_length=2,max_length=180)
 revision:int=Field(ge=0,le=1000000)
 action:Literal['upsert','revoke']
class BatchInput(BaseModel):
 model_config=ConfigDict(extra='forbid',allow_inf_nan=False)
 package_id:str
 note:str=Field(default='',max_length=300)
 tasks:list[TaskRecordInput]=Field(default_factory=list,max_length=200)
 dependencies:list[DependencyRecordInput]=Field(default_factory=list,max_length=400)
 @model_validator(mode='after')
 def check_nonempty(self):
  if not self.tasks and not self.dependencies:raise ValueError('批次至少包含一条任务或依赖记录')
  return self
