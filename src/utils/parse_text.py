import pandas as pd
import re

def parse_text(text):
    results = []
    if pd.isna(text):
        results.append({"variableelement": "valve (undiff)", 
                        "value": 1,
                        "context": None, 
                        "count": 'presence/absence'})
        return results
     
    # 'males & females' -> classify as specimen
    match_specimen = re.match(r'(\d+)\s*males?\s*&\s*females?', 
                              text, 
                              re.IGNORECASE)
    if match_specimen:
        num = int(match_specimen.group(1))
        results.append({"variableelement": "valve (undiff)", 
                        "value": num, 
                        "context": None,
                        "count": 'NISP'})
        return results
    
    matches = re.findall(r'(\d+)?\s*(male|female|specimen|males|females|specimens)', 
                         text, 
                         re.IGNORECASE)
    
    for num, category in matches:
        category = category.lower().rstrip('s')
        if category == 'female':
            category = 'valve (female)'
            context = None
        elif category == 'male':
            category = 'valve (male)'
            context = None
        else:
            category = 'valve (undiff)'
            context = None
        if num:
            num = int(num) 
            count = "NISP"
        else:
            num = 1
            count = "presence/absence"
        
        results.append({"variableelement": category, "value": num, "context": context, "count": count})
    
    return results