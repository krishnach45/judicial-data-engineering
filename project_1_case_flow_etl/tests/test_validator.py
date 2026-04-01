import pytest
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from project_1_case_flow_etl.validators.case_validator import CaseValidator

validator = CaseValidator()

def test_valid_record_passes():
    df = pd.DataFrame([{
        'case_number': 'CR-2024-00001',
        'case_type':   'criminal',
        'filed_date':  '2024-01-01',
        'status':      'open'
    }])
    result = validator.validate_dataframe(df)
    assert result['valid_count'] == 1
    assert result['invalid_count'] == 0
    assert result['pass_rate'] == 100.0

def test_invalid_case_number_fails():
    df = pd.DataFrame([{
        'case_number': 'AB',
        'case_type':   'criminal',
        'filed_date':  '2024-01-01',
        'status':      'open'
    }])
    result = validator.validate_dataframe(df)
    assert result['invalid_count'] == 1

def test_invalid_case_type_fails():
    df = pd.DataFrame([{
        'case_number': 'CR-2024-00001',
        'case_type':   'unknown_type',
        'filed_date':  '2024-01-01',
        'status':      'open'
    }])
    result = validator.validate_dataframe(df)
    assert result['invalid_count'] == 1

def test_null_case_type_passes():
    df = pd.DataFrame([{
        'case_number': 'CR-2024-00001',
        'case_type':   None,
        'filed_date':  '2024-01-01',
        'status':      'open'
    }])
    result = validator.validate_dataframe(df)
    assert result['valid_count'] == 1

def test_multiple_records_mixed():
    df = pd.DataFrame([
        {'case_number': 'CR-2024-00001', 'case_type': 'criminal',
         'filed_date': '2024-01-01', 'status': 'open'},
        {'case_number': 'AB', 'case_type': 'criminal',
         'filed_date': '2024-01-01', 'status': 'open'},
        {'case_number': 'CR-2024-00002', 'case_type': 'invalid',
         'filed_date': '2024-01-01', 'status': 'open'},
    ])
    result = validator.validate_dataframe(df)
    assert result['valid_count'] == 1
    assert result['invalid_count'] == 2
