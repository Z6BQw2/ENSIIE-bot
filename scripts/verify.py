#!/usr/bin/env python3
"""Vérification qualité du pipeline de pré-traitement"""

import json
import sqlite3
from pathlib import Path
from collections import Counter


DB_PATH = "./data/documents.db"
EXPORT_PATH = "./data/export/all_documents.jsonl"


def check_db():
    """Vérifications sur la base SQLite"""
    print("=" * 60)
    print("1. BASE DE DONNÉES")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM documents").fetchall()

    print(f"\nTotal documents: {len(rows)}")

    # --- Champs vides ou nuls ---
    empty_fields = Counter()
    for row in rows:
        for field in ["content", "title", "source_url", "language", "source_type"]:
            val = row[field]
            if not val or (isinstance(val, str) and not val.strip()):
                empty_fields[field] += 1

    if empty_fields:
        print("\n⚠️  Champs vides/nuls:")
        for field, count in empty_fields.items():
            print(f"   {field}: {count} documents")
    else:
        print("\n✅ Aucun champ critique vide")

    # --- Langues ---
    langs = Counter(row["language"] for row in rows)
    print(f"\nPar langue: {dict(langs)}")

    # --- Sources ---
    sources = Counter(row["source_type"] for row in rows)
    print(f"Par source: {dict(sources)}")

    # --- Tailles de contenu ---
    sizes = [row["char_count"] for row in rows]
    print(f"\nTaille contenu (chars):")
    print(f"  Min:     {min(sizes)}")
    print(f"  Max:     {max(sizes)}")
    print(f"  Moyenne: {sum(sizes) // len(sizes)}")

    # --- Détection de résidus HTML ---
    print("\n" + "=" * 60)
    print("2. QUALITÉ DU NETTOYAGE")
    print("=" * 60)

    html_residues = []
    for row in rows:
        content = row["content"]
        # Chercher des balises HTML résiduelles
        for pattern in ["<div", "<span", "<script", "<style", "&nbsp;", "&#"]:
            if pattern in content.lower():
                html_residues.append((row["id"][:12], row["source_url"][:60], pattern))
                break

    if html_residues:
        print(f"\n⚠️  {len(html_residues)} documents avec résidus HTML:")
        for doc_id, url, pattern in html_residues[:10]:
            print(f"   {doc_id}... | {pattern} | {url}")
    else:
        print("\n✅ Aucun résidu HTML détecté")

    # --- Contenu trop court ou suspect ---
    short_docs = [(row["id"][:12], row["char_count"], row["source_url"][:60])
                  for row in rows if row["char_count"] < 100]

    if short_docs:
        print(f"\n⚠️  {len(short_docs)} documents très courts (<100 chars):")
        for doc_id, size, url in short_docs:
            print(f"   {doc_id}... | {size} chars | {url}")
    else:
        print("\n✅ Aucun document trop court")

    # --- Doublons de contenu (vérification supplémentaire) ---
    print("\n" + "=" * 60)
    print("3. DOUBLONS")
    print("=" * 60)

    contents = [row["content"][:200] for row in rows]  # Comparer les débuts
    content_counter = Counter(contents)
    near_dupes = [(text[:80], count) for text, count in content_counter.items() if count > 1]

    if near_dupes:
        print(f"\n⚠️  {len(near_dupes)} quasi-doublons (même début de contenu):")
        for text, count in near_dupes[:5]:
            print(f"   x{count} | \"{text}...\"")
    else:
        print("\n✅ Aucun quasi-doublon détecté")

    # --- IDs uniques ---
    ids = [row["id"] for row in rows]
    if len(ids) == len(set(ids)):
        print("✅ Tous les IDs sont uniques")
    else:
        print(f"⚠️  {len(ids) - len(set(ids))} IDs dupliqués!")

    conn.close()
    return rows


def check_export():
    """Vérifications sur l'export JSONL"""
    print("\n" + "=" * 60)
    print("4. EXPORT JSONL")
    print("=" * 60)

    if not Path(EXPORT_PATH).exists():
        print("❌ Fichier export introuvable")
        return

    docs = []
    with open(EXPORT_PATH, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            try:
                doc = json.loads(line)
                docs.append(doc)
            except json.JSONDecodeError:
                print(f"⚠️  Ligne {i}: JSON invalide")

    print(f"\nDocuments dans l'export: {len(docs)}")

    # Vérifier la structure
    required_keys = {"id", "text", "metadata"}
    required_meta = {"source_url", "source_type", "title", "language"}

    malformed = 0
    for doc in docs:
        if not required_keys.issubset(doc.keys()):
            malformed += 1
            continue
        if not required_meta.issubset(doc.get("metadata", {}).keys()):
            malformed += 1

    if malformed:
        print(f"⚠️  {malformed} documents mal formés")
    else:
        print("✅ Structure JSONL correcte pour tous les documents")

    # Vérifier que le texte n'est pas vide
    empty_text = sum(1 for d in docs if not d.get("text", "").strip())
    if empty_text:
        print(f"⚠️  {empty_text} documents avec texte vide")
    else:
        print("✅ Aucun document avec texte vide")


def check_samples(rows):
    """Affiche des échantillons pour vérification manuelle"""
    print("\n" + "=" * 60)
    print("5. ÉCHANTILLONS (vérification visuelle)")
    print("=" * 60)

    # Un de chaque source type
    seen_types = set()
    for row in rows:
        src = row["source_type"]
        if src not in seen_types:
            seen_types.add(src)
            print(f"\n--- {src.upper()} ---")
            print(f"  URL:    {row['source_url'][:80]}")
            print(f"  Titre:  {row['title'][:60] if row['title'] else '(vide)'}")
            print(f"  Langue: {row['language']}")
            print(f"  Taille: {row['char_count']} chars / {row['word_count']} mots")
            print(f"  Début:  \"{row['content'][:150]}...\"")

    # Le plus court et le plus long
    sorted_rows = sorted(rows, key=lambda r: r["char_count"])

    print("\n--- PLUS COURT ---")
    r = sorted_rows[0]
    print(f"  {r['source_url'][:80]}")
    print(f"  {r['char_count']} chars: \"{r['content'][:200]}\"")

    print("\n--- PLUS LONG ---")
    r = sorted_rows[-1]
    print(f"  {r['source_url'][:80]}")
    print(f"  {r['char_count']} chars: \"{r['content'][:200]}...\"")


def check_ocr():
    """Vérifie si l'OCR a été appliqué"""
    print("\n" + "=" * 60)
    print("6. OCR")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT source_url, metadata FROM documents WHERE source_type = 'pdf'").fetchall()

    ocr_count = 0
    for url, meta_str in rows:
        meta = json.loads(meta_str)
        if meta.get("ocr_applied"):
            ocr_count += 1
            print(f"  🔍 OCR appliqué: {url}")

    print(f"\n{ocr_count} PDFs traités par OCR sur {len(rows)} PDFs total")
    conn.close()


def main():
    rows = check_db()
    check_export()
    check_samples(rows)
    check_ocr()

    print("\n" + "=" * 60)
    print("VÉRIFICATION TERMINÉE")
    print("=" * 60)


if __name__ == "__main__":
    main()
