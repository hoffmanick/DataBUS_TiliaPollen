import re
from collections import defaultdict

def process_dataset_notes(text):
    
    entries = defaultdict(lambda: {"names": [], "fields": defaultdict(set)})  # Store all dynamic fields
    unassigned_entries = {"names": [], "fields": defaultdict(set)}  # Store ungrouped records separately

    # Split by '|&' to correctly parse each record
    records = text.strip().split("|&")

    for record in records:
        record = record.strip()
        if not record:
            continue  # Skip empty records

        # Extract key-value pairs dynamically
        key_value_pairs = re.findall(r"([^|:]+):\s*([^|]+)", record)  # 🚀 FIX: Ensures correct key-value extraction

        extracted_data = {}
        name_text = None
        primary_group_field = None

        for key, value in key_value_pairs:
            key = key.strip()
            value = value.strip()

            if "Name in Publication" in key:
                name_text = value  # Save the name separately
            elif "males and females text" in key.lower():  # Ensure it's added to the name
                name_text += f" {value}"
            else:
                extracted_data[key] = value
                if "sample number" in key.lower():  # Dynamically detect grouping field
                    primary_group_field = value

        # Store extracted data in the appropriate group
        if primary_group_field:
            entries[primary_group_field]["names"].append(name_text)
            for key, value in extracted_data.items():
                if key.lower() != "original sample number":  # 🚀 FIX: Prevent duplicate sample number
                    entries[primary_group_field]["fields"][key].add(value)
        else:
            unassigned_entries["names"].append(name_text)
            for key, value in extracted_data.items():
                unassigned_entries["fields"][key].add(value)

    # Step 2: Construct the final output dynamically
    output_strings = []

    # Process grouped entries
    for group, data in entries.items():
        base_string = f"Original Sample Number: {group} | Name in Publication: {'; '.join(data['names'])}"

        # Append additional fields only if they have a single unique value
        extra_info = [f"{key}: {list(values)[0]}" for key, values in data["fields"].items() if len(values) == 1]

        # 🚀 FIX: Only add extra fields if they exist to prevent "| |"
        if extra_info:
            base_string += " | " + " | ".join(extra_info)

        output_strings.append(base_string)

    # Process unassigned entries (without sample numbers)
    if unassigned_entries["names"]:
        unassigned_string = f"Name in Publication: {', '.join(unassigned_entries['names'])}"

        # Append additional fields if they exist
        extra_info = [f"{key}: {list(values)[0]}" for key, values in unassigned_entries["fields"].items() if len(values) == 1]

        # 🚀 FIX: Only add extra fields if they exist
        if extra_info:
            unassigned_string += " | ".join(extra_info)

        output_strings.append(unassigned_string)

    # Final formatted string
    final_output = "\n".join(output_strings)
    return final_output