from sqlalchemy import String,Integer,JSON,ForeignKey,Text,UniqueConstraint
from sqlalchemy.orm import Mapped,mapped_column
from src.config.database import Base
class ImportBatch(Base):
 __tablename__='import_batches'
 id:Mapped[str]=mapped_column(String(80),primary_key=True)
 package_id:Mapped[str]=mapped_column(ForeignKey('work_packages.id'))
 note:Mapped[str]=mapped_column(String(300),default='')
 actor:Mapped[str]=mapped_column(String(160))
 status:Mapped[str]=mapped_column(String(16))
 error:Mapped[str]=mapped_column(Text,default='')
 summary:Mapped[dict]=mapped_column(JSON,default=dict)
 results:Mapped[list]=mapped_column(JSON,default=list)
 created_at:Mapped[str]=mapped_column(String(64))
class ImportRecord(Base):
 __tablename__='import_records'
 id:Mapped[str]=mapped_column(String(80),primary_key=True)
 __table_args__=(UniqueConstraint('package_id','source','kind','stable_id'),)
 package_id:Mapped[str]=mapped_column(ForeignKey('work_packages.id'))
 source:Mapped[str]=mapped_column(String(180))
 kind:Mapped[str]=mapped_column(String(16))
 stable_id:Mapped[str]=mapped_column(String(80))
 revision:Mapped[int]=mapped_column(Integer)
 content:Mapped[dict]=mapped_column(JSON,nullable=True)
 content_hash:Mapped[str]=mapped_column(String(64))
 status:Mapped[str]=mapped_column(String(16))
 batch_id:Mapped[str]=mapped_column(String(80))
 updated_at:Mapped[str]=mapped_column(String(64))
