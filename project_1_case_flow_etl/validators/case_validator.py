import pandas as pd
import logging
from typing import Optional
from pydantic import BaseModel, field_validator, ValidationError

logger = logging.getLogger(__name__)

class CaseRecord(BaseModel):
    case_number: str
    case_type:   Optional[str] = None
    filed_date:  Optional[str] = None
    status:      Optional[str] = None

    @field_validator('case_number')
    @classmethod
    def validate_case_number(cls, v):
        if not v or len(str(v).strip()) < 3:
            raise ValueError('case_number must be at least 3 characters')
        return v

    @field_validator('case_type')
    @classmethod
    def validate_case_type(cls, v):
        if v is None:
            return v
        valid = {'criminal', 'civil', 'family', 'traffic', 'probate'}
        if v.lower() not in valid:
            raise ValueError(f'Invalid case_type: {v}')
        return v.lower()

class CaseValidator:

    def validate_dataframe(self, df: pd.DataFrame) -> dict:
        errors        = []
        valid_indices = []

        for idx, row in df.iterrows():
            try:
                CaseRecord(
                    case_number=str(row.get('case_number', '')),
                    case_type=row.get('case_type'),
                    filed_date=str(row.get('filed_date')) if pd.notna(row.get('filed_date')) else None,
                    status=row.get('status'),
                )
                valid_indices.append(idx)
            except ValidationError as e:
                errors.append({'row': row.to_dict(), 'error': str(e)})

        total     = len(df)
        valid     = len(valid_indices)
        invalid   = len(errors)
        pass_rate = (valid / total * 100) if total > 0 else 0

        logger.info(f"Validation: {valid:,} passed | {invalid:,} failed | Pass rate: {pass_rate:.1f}%")

        return {
            'valid_df':      df.loc[valid_indices],
            'valid_count':   valid,
            'invalid_count': invalid,
            'pass_rate':     pass_rate,
            'errors':        errors
        }
