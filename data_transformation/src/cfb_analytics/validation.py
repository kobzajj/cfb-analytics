import pandas as pd

def validate_passing(df: pd.DataFrame): 
    issues = []
    if (df['completions'] > df['pass_attempts']).any(): issues.append('completions > attempts')
    if (df['pressures_faced'] < df['sacks_taken']).any(): issues.append('pressures_faced < sacks_taken')
    return issues

def validate_rushing(df: pd.DataFrame): 
    return []

def validate_receiving(df: pd.DataFrame):
    issues = []
    if 'routes' in df.columns and (df['routes'].notna() & (df['routes'] < df['targets'])).any():
        issues.append('routes < targets')
    return issues

def validate_defense(df: pd.DataFrame):
    issues = []
    if {'receptions_allowed','targets'}.issubset(df.columns):
        if (df['receptions_allowed'] > df['targets']).any():
            issues.append('receptions_allowed > targets')
    return issues
