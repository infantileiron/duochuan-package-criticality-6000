from fastapi import APIRouter,Depends
from sqlalchemy import select
from src.config.database import db_session
from src.controllers.auth import current_user,writer
from src.models import ImportBatch
from src.schemas.batch import BatchInput
from src.services import batches as service
from src.utils.serialization import rows
router=APIRouter(prefix='/batches')
@router.get('')
def listing(db=Depends(db_session),user=Depends(current_user)):return rows(db.scalars(select(ImportBatch).order_by(ImportBatch.created_at.desc())))
@router.post('')
def create(payload:BatchInput,db=Depends(db_session),user=Depends(writer)):return service.create(db,payload,user['email'])
