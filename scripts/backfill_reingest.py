"""Backfill / re-ingest script (Phase 9, plan §6/§11).

Dev data is disposable, but the vector store's metadata shape has changed
across the overhaul (taxonomy booleans, explicit parent embeddings, real
importance_score instead of the old hardcoded default). Old `news_archive`
entries don't have the new `topic_<slug>` boolean keys, so brief_engine's
topic_filter()-based queries would silently see them as matching nothing.

This script:
  1. Deletes the ChromaDB `news_archive` collection outright (no partial
     migration — see docs/OVERHAUL_PLAN.md §6).
  2. Prints the follow-up steps to re-populate it: run the producer once,
     then let the triage and storage consumers drain the resulting messages
     (they now write v2 metadata on the way in).

Usage:
    python scripts/backfill_reingest.py           # prompts for confirmation
    python scripts/backfill_reingest.py --yes      # skips the prompt
"""
import argparse
import sys

from newsagg.storage.vector_store import COLLECTION_NAME, get_chroma_client


def delete_collection(client) -> bool:
    """Deletes the news_archive collection if it exists. Returns True if a
    collection was actually deleted, False if there was nothing to delete.

    ChromaDB 0.6.x's list_collections() returns plain collection name
    strings (not Collection objects — accessing `.name` on an entry raises
    NotImplementedError, see chromadb's v0.6.0 migration notes), so this
    compares names directly.
    """
    existing_names = set(client.list_collections())
    if COLLECTION_NAME not in existing_names:
        return False
    client.delete_collection(name=COLLECTION_NAME)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yes", action="store_true", help="Skip the interactive confirmation prompt."
    )
    args = parser.parse_args()

    if not args.yes:
        answer = input(
            f"This will permanently delete the '{COLLECTION_NAME}' ChromaDB "
            "collection (all ingested articles). Continue? [y/N] "
        )
        if answer.strip().lower() not in ("y", "yes"):
            print("Aborted — nothing was deleted.")
            sys.exit(1)

    client = get_chroma_client()
    deleted = delete_collection(client)

    if deleted:
        print(f"Deleted ChromaDB collection '{COLLECTION_NAME}'.")
    else:
        print(f"No existing '{COLLECTION_NAME}' collection found — nothing to delete.")

    print(
        "\nNext steps to re-populate the vector store with the new (v2) "
        "metadata shape:\n"
        "  1. Make sure infra is up:      docker compose up -d\n"
        "  2. Run the producer once:      newsagg-producer   (or: python -m newsagg.ingestion.producer)\n"
        "  3. Let it drain through the triage + storage consumers, either via\n"
        "     ./scripts/dev.sh or by running newsagg-triage / newsagg-storage\n"
        "     directly — both now write taxonomy booleans, explicit parent\n"
        "     embeddings, and the real triage importance_score on the way in.\n"
    )


if __name__ == "__main__":
    main()
