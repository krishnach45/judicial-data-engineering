import pandas as pd
import random
import hashlib 
from faker import Faker
from pathlib import Path
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)      #Reproducible - same data every run
random.seed(42)

CASE_TYPES = ['criminal','civil','family','traffic','probate']
CASE_STATUSES = ['open','closed','pending','dismissed','appealed']
PARTY_TYPES = ['defendant','plantiff','attorney','witness']
CHARGE_TYPES = ['felony','misdemanor','infraction','violation']
HEARING_TYPES = ['arraignment','preliminary','trail','sentencing','appeal']
OUTCOMES      = ['continued','resolved','dismissed','guilty','not_guilty']

# -- intentional DATA QUALITY PROBLEMS 
#  these are what we actuall find in the court systems
#
DIRTY_DATE_FORMATS = ['%m%d%Y' , '%Y-%m-%d', '%d-%b-%Y', '%m-%d-%y' ]
DIRTY_DATE_PREFIXES = ['CR','cr','cr','CRIM','']

def dirty_date(dt):
    """Return a date in a random format - simulates leagacy export inconsistency"""
    fmt =random.choice(DIRTY_DATE_FORMATS)
    return dt.strftime(fmt)

def maybe_null(value, null_rate = 0.05):
    """Randomly nullify a value - simulates missing data from source systems"""
    return None if random.random() < null_rate else value

def dirty_case_number(year, seq):
    """ Generate Inconsistently formatted case numbers - like real legacy exports"""
    prefix = random.choice(DIRTY_DATE_PREFIXES)
    sep  = random.choice(['-','/',' ',''])
    return f"{prefix}{sep}{year}{sep}{str(seq).zfill(5)}"


# Generate Cases
def generate_cases(n=10000):
    cases = []
    for i in range(n):
        filed = fake.date_between(start_date='-5y', end_date = 'today')
        year = filed.year


        # Introduce ~3% duplicates to test deduplication logic
        if i > 0 and random.random() < 0.03:
            case_num = cases[random.randint(0,i-1)]['case_number']
        else:
            case_num = dirty_case_number(year,i)


        cases.append({
            'case_number' : case_num,
            'case_type' : maybe_null(random.choice(CASE_TYPES),0.02),
            'filed_date' : dirty_date(filed),         # Inconsistent date format
            'status' : maybe_null (random.choice(CASE_STATUSES),0.04),
            'court_id' : random.randint(1, 20),
            'Judge_id' : maybe_null(random.randint(1,50),0.08),
            'ssn' : fake.ssn(),
            'notes': maybe_null(fake.sentence(),0.60)

        })
    return pd.DataFrame(cases)
    

# ── GENERATE PARTIES ───────────────────────────────────────────────────────
def generate_parties(cases_df):
    parties = []
    for _, case in cases_df.iterrows():
        n_parties = random.randint(1, 4)
        for _ in range(n_parties):
            dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
            parties.append({
                'case_number': case['case_number'],
                'party_type':  random.choice(PARTY_TYPES),
                'full_name':   maybe_null(fake.name(), 0.01),
                'dob':         maybe_null(dirty_date(dob), 0.10),
                'ssn':         maybe_null(fake.ssn(), 0.15),
                'address':     maybe_null(fake.address().replace('\n', ', '), 0.12),
                'phone':       maybe_null(fake.phone_number(), 0.20),
            })
    return pd.DataFrame(parties)


# ── GENERATE HEARINGS ──────────────────────────────────────────────────────
def generate_hearings(cases_df):
    hearings = []
    for _, case in cases_df.iterrows():
        n_hearings = random.randint(0, 6)
        for h in range(n_hearings):
            scheduled = fake.date_time_between(start_date='-4y', end_date='+6m')
            # ~15% of hearings have no actual date (future or skipped)
            actual = maybe_null(
                scheduled + timedelta(minutes=random.randint(-30, 120)),
                null_rate=0.15
            )
            hearings.append({
                'case_number':  case['case_number'],
                'hearing_type': random.choice(HEARING_TYPES),
                'scheduled_dt': scheduled.strftime('%Y-%m-%d %H:%M:%S'),
                'actual_dt':    actual.strftime('%Y-%m-%d %H:%M:%S') if actual else None,
                'outcome':      maybe_null(random.choice(OUTCOMES), 0.25),
                'room':         maybe_null(f'Courtroom {random.randint(1,30)}', 0.10),
            })
    return pd.DataFrame(hearings)


# ── GENERATE CHARGES ───────────────────────────────────────────────────────
def generate_charges(cases_df):
    charges = []
    criminal = cases_df[cases_df['case_type'] == 'criminal']
    for _, case in criminal.iterrows():
        n_charges = random.randint(1, 5)
        for _ in range(n_charges):
            charges.append({
                'case_number':   case['case_number'],
                'charge_type':   random.choice(CHARGE_TYPES),
                'statute_code':  f'{random.randint(100,999)}.{random.randint(1,99)}',
                'description':   fake.sentence(nb_words=6),
                'disposition':   maybe_null(random.choice(OUTCOMES), 0.30),
                'sentence_days': maybe_null(random.randint(0, 3650), 0.50),
            })
    return pd.DataFrame(charges)


# ── GENERATE JUDGES ────────────────────────────────────────────────────────
def generate_judges(n=50):
    return pd.DataFrame([{
        'judge_id':     i + 1,
        'full_name':    f'Hon. {fake.name()}',
        'court_id':     random.randint(1, 20),
        'appointed_dt': fake.date_between(start_date='-20y', end_date='-1y'),
        'active':       random.choice([True, True, True, False]),
    } for i in range(n)])


# ── MAIN ───────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    output_dir = Path('data/synthetic')
    output_dir.mkdir(parents=True, exist_ok=True)

    print('Generating 10,000 court cases...')
    cases    = generate_cases(10000)
    parties  = generate_parties(cases)
    hearings = generate_hearings(cases)
    charges  = generate_charges(cases)
    judges   = generate_judges(50)

    cases.to_csv(output_dir / 'cases.csv',       index=False)
    parties.to_csv(output_dir / 'parties.csv',   index=False)
    hearings.to_csv(output_dir / 'hearings.csv', index=False)
    charges.to_csv(output_dir / 'charges.csv',   index=False)
    judges.to_csv(output_dir / 'judges.csv',     index=False)

    print(f'Generated:')
    print(f'  Cases:    {len(cases):,} rows')
    print(f'  Parties:  {len(parties):,} rows')
    print(f'  Hearings: {len(hearings):,} rows')
    print(f'  Charges:  {len(charges):,} rows')
    print(f'  Judges:   {len(judges):,} rows')
    print(f'Saved to {output_dir}/')