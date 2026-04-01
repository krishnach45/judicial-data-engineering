import pytest
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from project_1_case_flow_etl.transformers.case_transformer import CaseTransformer

transformer = CaseTransformer()

def test_cleans_case_numbers():
    df     = pd.DataFrame({'case_number': ['  cr-2024-001 ', 'CRIM/2024/002']})
    result = transformer._clean_case_numbers(df)
    assert result['case_number'].iloc[0] == 'CR-2024-001'

def test_hashes_ssn():
    df     = pd.DataFrame({'ssn': ['123-45-6789']})
    result = transformer._hash_pii(df)
    assert 'ssn' not in result.columns
    assert 'ssn_hash' in result.columns
    assert len(result['ssn_hash'].iloc[0]) == 64

def test_removes_duplicates():
    df = pd.DataFrame({
        'case_number': ['A1', 'A1', 'B2'],
        'status':      ['open', 'closed', 'open']
    })
    result = transformer._remove_duplicates(df)
    assert len(result) == 2

def test_standardizes_case_type():
    df     = pd.DataFrame({'case_type': ['CRIMINAL', ' Civil ', 'FAMILY']})
    result = transformer._standardize_categories(df)
    assert result['case_type'].tolist() == ['criminal', 'civil', 'family']

def test_null_case_number_returns_none():
    df     = pd.DataFrame({'case_number': [None, '', '   ']})
    result = transformer._clean_case_numbers(df)
    assert result['case_number'].isna().all()

def test_drops_system_columns():
    df = pd.DataFrame({
        'case_number': ['CR-001'],
        '_loaded_at':  ['2024-01-01'],
        '_processed':  ['false'],
        '_error_msg':  [None],
        '_run_id':     [None]
    })
    result = transformer._drop_system_columns(df)
    assert '_loaded_at' not in result.columns
    assert '_processed' not in result.columns
    assert 'case_number' in result.columns

def test_full_transform_pipeline():
    df = pd.DataFrame({
        'case_number': ['cr-2024-001', 'cr-2024-001', 'CRIM/2024/002'],
        'case_type':   ['CRIMINAL', 'criminal', 'CIVIL'],
        'filed_date':  ['2024-01-01', '2024-01-01', '2024-02-01'],
        'status':      ['open', 'open', 'closed'],
        'court_id':    ['1', '1', '2'],
        'judge_id':    ['10', '10', '20'],
        'notes':       [None, None, 'note'],
        'ssn':         ['123-45-6789', '123-45-6789', '987-65-4321'],
    })
    result = transformer.transform(df)
    assert len(result) == 2
    assert 'ssn' not in result.columns
    assert 'ssn_hash' in result.columns
    assert result['case_type'].iloc[0] == 'criminal'
