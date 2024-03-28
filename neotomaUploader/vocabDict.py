from collections import defaultdict
import json
import logging

def vocabDict(data):
    """
    Obtain the units dictionary
    """

    vocab_dict = {d['column']: d['vocab'] for d in data \
                  if 'vocab' in d and d['vocab'] is not None}

    # Convert list of strings from XLSX to proepr lists
    for key, value in vocab_dict.items():
        try:
            vocab_dict[key] = json.loads(value)
        except json.JSONDecodeError:
            logging.error(f"Vocabulary list for {key} cannot be decoded. \
                              Is it a string?")
    
    # Maybe we can remove this part
    unit_cols = defaultdict(list)
    for d in data:
        if 'class' in d.keys():
            unit_cols[d['class']].append(d['column'])
    unit_cols = dict(unit_cols)

    return vocab_dict