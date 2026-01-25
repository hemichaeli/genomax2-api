#!/usr/bin/env python3
"""
GenoMAX² Migration Runner
=========================
Runs pending SQL migrations on startup.

Usage:
    python scripts/run_migrations.py
    
Or import and call:
    from scripts.run_migrations import run_pending_migrations
    run_pending_migrations()
"""

import os
import sys
import re
import logging
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Get database connection from environment variables."""
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url:
        return psycopg2.connect(database_url)
    
    # Fallback to individual env vars
    return psycopg2.connect(
        host=os.environ.get('PGHOST', 'localhost'),
        port=os.environ.get('PGPORT', '5432'),
        database=os.environ.get('PGDATABASE', 'railway'),
        user=os.environ.get('PGUSER', 'postgres'),
        password=os.environ.get('PGPASSWORD', '')
    )


def ensure_migrations_table(conn):
    """Create migrations tracking table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS _migrations (
                id SERIAL PRIMARY KEY,
                filename VARCHAR(255) UNIQUE NOT NULL,
                executed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                checksum VARCHAR(64),
                success BOOLEAN DEFAULT true,
                error_message TEXT
            )
        """)
        conn.commit()
    logger.info("Migrations table ready")


def get_executed_migrations(conn):
    """Get list of already executed migrations."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT filename FROM _migrations 
            WHERE success = true 
            ORDER BY filename
        """)
        return {row['filename'] for row in cur.fetchall()}


def get_pending_migrations(migrations_dir: Path, executed: set):
    """Get list of pending migration files."""
    if not migrations_dir.exists():
        logger.warning(f"Migrations directory not found: {migrations_dir}")
        return []
    
    # Find all .sql files with numeric prefix
    migration_files = []
    for f in migrations_dir.glob("*.sql"):
        # Match files like 001_name.sql, 013_bloodwork.sql
        if re.match(r'^\d+_', f.name):
            if f.name not in executed:
                migration_files.append(f)
    
    # Sort by filename (numeric prefix ensures correct order)
    return sorted(migration_files, key=lambda f: f.name)


def calculate_checksum(content: str) -> str:
    """Calculate SHA256 checksum of migration content."""
    import hashlib
    return hashlib.sha256(content.encode()).hexdigest()


def run_migration(conn, migration_file: Path) -> bool:
    """Run a single migration file."""
    logger.info(f"Running migration: {migration_file.name}")
    
    content = migration_file.read_text()
    checksum = calculate_checksum(content)
    
    try:
        with conn.cursor() as cur:
            # Execute the migration
            cur.execute(content)
            
            # Record successful migration
            cur.execute("""
                INSERT INTO _migrations (filename, checksum, success)
                VALUES (%s, %s, true)
                ON CONFLICT (filename) DO UPDATE SET
                    executed_at = NOW(),
                    checksum = EXCLUDED.checksum,
                    success = true,
                    error_message = NULL
            """, (migration_file.name, checksum))
            
        conn.commit()
        logger.info(f"✓ Migration {migration_file.name} completed successfully")
        return True
        
    except Exception as e:
        conn.rollback()
        error_msg = str(e)
        logger.error(f"✗ Migration {migration_file.name} failed: {error_msg}")
        
        # Record failed migration
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO _migrations (filename, checksum, success, error_message)
                    VALUES (%s, %s, false, %s)
                    ON CONFLICT (filename) DO UPDATE SET
                        executed_at = NOW(),
                        checksum = EXCLUDED.checksum,
                        success = false,
                        error_message = EXCLUDED.error_message
                """, (migration_file.name, checksum, error_msg))
            conn.commit()
        except:
            pass
        
        return False


def run_pending_migrations(migrations_dir: str = None) -> dict:
    """
    Run all pending migrations.
    
    Args:
        migrations_dir: Path to migrations directory. 
                       Defaults to ./migrations relative to project root.
    
    Returns:
        dict with 'success', 'executed', 'failed', 'skipped' counts
    """
    # Determine migrations directory
    if migrations_dir:
        mig_path = Path(migrations_dir)
    else:
        # Default: look for migrations/ relative to this script or project root
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        mig_path = project_root / "migrations"
    
    logger.info(f"Migrations directory: {mig_path}")
    
    result = {
        'success': True,
        'executed': 0,
        'failed': 0,
        'skipped': 0,
        'errors': []
    }
    
    try:
        conn = get_db_connection()
        logger.info("Database connection established")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        result['success'] = False
        result['errors'].append(str(e))
        return result
    
    try:
        # Ensure migrations tracking table exists
        ensure_migrations_table(conn)
        
        # Get already executed migrations
        executed = get_executed_migrations(conn)
        logger.info(f"Previously executed migrations: {len(executed)}")
        
        # Get pending migrations
        pending = get_pending_migrations(mig_path, executed)
        logger.info(f"Pending migrations: {len(pending)}")
        
        if not pending:
            logger.info("No pending migrations to run")
            return result
        
        # Run each pending migration
        for migration_file in pending:
            if run_migration(conn, migration_file):
                result['executed'] += 1
            else:
                result['failed'] += 1
                result['success'] = False
                result['errors'].append(f"Failed: {migration_file.name}")
                # Stop on first failure to maintain consistency
                break
        
        # Count skipped (remaining after failure)
        result['skipped'] = len(pending) - result['executed'] - result['failed']
        
    except Exception as e:
        logger.error(f"Migration runner error: {e}")
        result['success'] = False
        result['errors'].append(str(e))
    finally:
        conn.close()
    
    # Summary
    logger.info(f"Migration summary: {result['executed']} executed, {result['failed']} failed, {result['skipped']} skipped")
    
    return result


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run database migrations')
    parser.add_argument('--dir', '-d', help='Migrations directory path')
    parser.add_argument('--dry-run', action='store_true', help='Show pending migrations without running')
    args = parser.parse_args()
    
    if args.dry_run:
        # Just show what would be run
        mig_path = Path(args.dir) if args.dir else Path(__file__).parent.parent / "migrations"
        try:
            conn = get_db_connection()
            ensure_migrations_table(conn)
            executed = get_executed_migrations(conn)
            pending = get_pending_migrations(mig_path, executed)
            conn.close()
            
            print(f"\nExecuted migrations: {len(executed)}")
            for m in sorted(executed):
                print(f"  ✓ {m}")
            
            print(f"\nPending migrations: {len(pending)}")
            for m in pending:
                print(f"  ○ {m.name}")
            
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
    else:
        result = run_pending_migrations(args.dir)
        sys.exit(0 if result['success'] else 1)


if __name__ == '__main__':
    main()
