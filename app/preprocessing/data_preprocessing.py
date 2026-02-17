import os
import json
import psycopg2
import shutil
from pathlib import Path
from huggingface_hub import hf_hub_download

# ----------------------
# Config
# ----------------------
BASE_PATH = Path("official/pdf/arxiv")
REPO_ID = "vectara/open_ragbench"

DB_CONFIG = {
    "host": "db",
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# Metadaten
FILES = [
    "answers.json",
    "qrels.json",
    "queries.json",
]

# Splits
SPLITS = ["train", "validation", "test"]

# ----------------------
# 1️⃣ Metadaten-Dateien von Hugging Face herunterladen
# ----------------------
os.makedirs(BASE_PATH, exist_ok=True)
print("Lade Metadaten-Dateien von Hugging Face herunter...")

for fname in FILES:
    local_path = BASE_PATH / fname
    if not local_path.exists():
        hf_path = hf_hub_download(repo_id=REPO_ID, filename=f"pdf/arxiv/{fname}", repo_type="dataset")
        shutil.copy2(hf_path, local_path)  # Cross-device-safe
        print(f"{fname} heruntergeladen ✅")
    else:
        print(f"{fname} bereits vorhanden, überspringe...")

    # Anzahl der Einträge ausgeben
    with open(local_path, encoding="utf-8") as f:
        data = json.load(f)
        print(f"{fname}: {len(data)} Einträge")

# ----------------------
# 1️⃣b Corpus JSON pro Split herunterladen
# ----------------------
for split in SPLITS:
    combined_docs = []
    file_num = 0

    print(f"\nLade Corpus für Split: {split}...")

    while True:
        fname = f"{file_num:04d}.json"  # typische Namensstruktur
        try:
            hf_file_path = hf_hub_download(
                repo_id=REPO_ID,
                filename=f"pdf/arxiv/corpus/{split}/{fname}",
                repo_type="dataset"
            )
        except Exception:
            # keine Datei mehr → fertig
            break

        with open(hf_file_path, encoding="utf-8") as f:
            doc = json.load(f)
            combined_docs.append(doc)

        file_num += 1

    if combined_docs:
        # Speichern als eine große JSON pro Split
        combined_path = BASE_PATH / f"corpus_{split}.json"
        with open(combined_path, "w", encoding="utf-8") as f:
            json.dump(combined_docs, f, ensure_ascii=False, indent=2)

        print(f"Split {split}: {len(combined_docs)} Dokumente heruntergeladen und gespeichert in {combined_path}")
    else:
        print(f"Split {split}: keine Dokumente gefunden, überspringe...")

# ----------------------
# 2️⃣ DB Verbindung
# ----------------------
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# ----------------------
# 3️⃣ Alle Splits importieren
# ----------------------
for split in SPLITS:
    split_path = BASE_PATH / split
    corpus_path = split_path / "corpus"

    if corpus_path.exists():
        print(f"Importiere {split}-Split...")

        # Documents + Sections
        for filename in os.listdir(corpus_path):
            with open(corpus_path / filename, encoding="utf-8") as f:
                doc = json.load(f)

            cur.execute("""
                INSERT INTO documents (id, title, abstract, authors, categories, published, updated, split)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                doc["id"],
                doc["title"],
                doc.get("abstract"),
                doc.get("authors"),
                doc.get("categories"),
                doc.get("published"),
                doc.get("updated"),
                split
            ))

            for idx, section in enumerate(doc["sections"]):
                cur.execute("""
                    INSERT INTO sections (doc_id, section_index, text)
                    VALUES (%s,%s,%s)
                """, (doc["id"], idx, section["text"]))

        # Queries
        queries_file = split_path / "queries.json"
        if queries_file.exists():
            with open(queries_file, encoding="utf-8") as f:
                queries = json.load(f)

            for qid, qdata in queries.items():
                cur.execute("""
                    INSERT INTO queries (id, query, type, source, split)
                    VALUES (%s,%s,%s,%s,%s)
                """, (qid, qdata["query"], qdata["type"], qdata["source"], split))

        # Qrels
        qrels_file = split_path / "qrels.json"
        if qrels_file.exists():
            with open(qrels_file, encoding="utf-8") as f:
                qrels = json.load(f)

            for qid, rel in qrels.items():
                cur.execute("""
                    INSERT INTO qrels (query_id, doc_id, section_index)
                    VALUES (%s,%s,%s)
                """, (qid, rel["doc_id"], rel["section_id"]))

        # Answers
        answers_file = split_path / "answers.json"
        if answers_file.exists():
            with open(answers_file, encoding="utf-8") as f:
                answers = json.load(f)

            for qid, answer in answers.items():
                cur.execute("""
                    INSERT INTO answers (query_id, answer)
                    VALUES (%s,%s)
                """, (qid, answer))

        conn.commit()
    else:
        print(f"Split {split} existiert nicht, überspringe...")

cur.close()
conn.close()
print("Import aller Splits abgeschlossen!")
