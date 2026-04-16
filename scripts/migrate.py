"""
DB 마이그레이션 실행 스크립트.

Supabase SQL Editor에서 직접 실행하거나,
이 스크립트로 마이그레이션 파일을 순서대로 출력.

사용법:
    # 마이그레이션 SQL 출력 (복사 → Supabase SQL Editor에 붙여넣기)
    python scripts/migrate.py

    # 특정 마이그레이션만 출력
    python scripts/migrate.py --only 001
"""

import argparse
import os
import sys

MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "migrations")


def get_migration_files(only: str | None = None) -> list[str]:
    """마이그레이션 파일 목록 (번호 순)."""
    files = sorted(
        f for f in os.listdir(MIGRATIONS_DIR)
        if f.endswith(".sql")
    )
    if only:
        files = [f for f in files if f.startswith(only)]
    return files


def main():
    parser = argparse.ArgumentParser(description="DB 마이그레이션 SQL 출력")
    parser.add_argument("--only", type=str, help="특정 마이그레이션 번호만 (예: 001)")
    args = parser.parse_args()

    files = get_migration_files(args.only)

    if not files:
        print("마이그레이션 파일이 없습니다.")
        sys.exit(1)

    print("=" * 60)
    print("Signal AI — DB 마이그레이션")
    print("아래 SQL을 Supabase SQL Editor에서 실행하세요.")
    print("=" * 60)

    for fname in files:
        filepath = os.path.join(MIGRATIONS_DIR, fname)
        with open(filepath, "r", encoding="utf-8") as f:
            sql = f.read()

        print(f"\n-- ========== {fname} ==========")
        print(sql)

    print("\n-- ========== 마이그레이션 완료 ==========")
    print(f"-- 총 {len(files)}개 파일 실행")


if __name__ == "__main__":
    main()
