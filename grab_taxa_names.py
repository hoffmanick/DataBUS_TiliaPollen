# Path to your text file
file_path = "data/validation_logs/not_validated/LV4_pollen_combined.csv.valid.log"

# Words or phrases you want to remove
remove_words = ["✗  taxon ID for ", " not found. Does it exist in Neotoma?"]

# Output list
results = []

with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
        if "taxon ID for" in line:
            # Clean the line
            cleaned = line.strip()
            for w in remove_words:
                cleaned = cleaned.replace(w, "")
            # Remove extra spaces
            cleaned = " ".join(cleaned.split())
            results.append(cleaned)

# Print results in spreadsheet-friendly format
for r in results:
    print(r)
