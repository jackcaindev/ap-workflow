from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.schemas.extraction import ExtractionResult
from app.services.extraction import (
    ExtractionError,
    UnsupportedFileTypeError,
    extract_document,
)


router = APIRouter(tags=["extraction"])


@router.post("/extract", response_model=ExtractionResult)
async def extract(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_session),
) -> ExtractionResult:
    file_bytes = await file.read()

    try:
        return await extract_document(file_bytes, file.filename or "", db)
    except UnsupportedFileTypeError as exc:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=str(exc),
        ) from exc
    except ExtractionError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc
