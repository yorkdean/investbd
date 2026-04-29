"""Export database to JSON for frontend"""
import sqlite3
import json
from datetime import datetime

conn = sqlite3.connect(r'C:\Users\Dylan\WorkBuddy\20260423171621\investment_institutions.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Get all institutions
cur.execute("SELECT * FROM investment_institutions ORDER BY name")
institutions = []
for row in cur.fetchall():
    inst = dict(row)
    # Get tags for this institution
    cur.execute("""
        SELECT year, list_name, rank, source_org, category 
        FROM institution_tags 
        WHERE institution_id=? 
        ORDER BY year DESC, list_name
    """, (inst['id'],))
    tags = []
    for tag_row in cur.fetchall():
        tags.append(dict(tag_row))
    inst['tags'] = tags
    inst['tag_count'] = len(tags)
    institutions.append(inst)

# Get metadata stats
cur.execute("SELECT COUNT(*) FROM institution_tags")
total_tags = cur.fetchone()[0]

cur.execute("SELECT DISTINCT year FROM institution_tags WHERE year IS NOT NULL ORDER BY year")
years = [row[0] for row in cur.fetchall()]

cur.execute("SELECT DISTINCT source_org FROM institution_tags WHERE source_org IS NOT NULL AND source_org != '' ORDER BY source_org")
sources = [row[0] for row in cur.fetchall()]

cur.execute("SELECT DISTINCT category FROM institution_tags WHERE category IS NOT NULL AND category != '' ORDER BY category")
categories = [row[0] for row in cur.fetchall()]

# Write JSON
output = {
    "metadata": {
        "totalInstitutions": len(institutions),
        "totalTags": total_tags,
        "years": years,
        "sources": sources,
        "categories": categories,
        "generatedAt": datetime.now().isoformat()
    },
    "institutions": institutions
}

with open(r'C:\Users\Dylan\WorkBuddy\20260423171621\data.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"Exported {len(institutions)} institutions to data.json")

# Stats
cur.execute("SELECT source_org, COUNT(*) as cnt FROM institution_tags GROUP BY source_org ORDER BY cnt DESC")
print("\nSource distribution:")
for row in cur.fetchall():
    src = row[0] if row[0] else "Unknown"
    print(f"  {src:20s} {row[1]:5d}")

conn.close()
