import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine import Engine

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/portfolio_tracker")

engine_kwargs = {}
if DATABASE_URL.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, **engine_kwargs)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()


def _sqlite_table_exists(connection, table_name: str) -> bool:
    row = connection.execute(
        text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:name"),
        {"name": table_name},
    ).first()
    return bool(row)


def _sqlite_columns(connection, table_name: str) -> list[str]:
    if not _sqlite_table_exists(connection, table_name):
        return []
    rows = connection.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
    return [str(row[1]) for row in rows]


def _sqlite_unique_index_columns(connection, table_name: str) -> list[list[str]]:
    if not _sqlite_table_exists(connection, table_name):
        return []
    indexes = connection.execute(text(f"PRAGMA index_list({table_name})")).fetchall()
    unique_indexes: list[list[str]] = []
    for index_row in indexes:
        is_unique = int(index_row[2]) == 1
        if not is_unique:
            continue
        index_name = str(index_row[1])
        info_rows = connection.execute(text(f"PRAGMA index_info({index_name})")).fetchall()
        columns = [str(info_row[2]) for info_row in info_rows]
        unique_indexes.append(columns)
    return unique_indexes


def _sqlite_backfill_user_id(connection, table_name: str, default_user_id: int | None) -> None:
    if not _sqlite_table_exists(connection, table_name):
        return
    columns = _sqlite_columns(connection, table_name)
    if "user_id" not in columns:
        connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN user_id INTEGER"))
    if default_user_id is not None:
        connection.execute(
            text(f"UPDATE {table_name} SET user_id = :user_id WHERE user_id IS NULL"),
            {"user_id": default_user_id},
        )


def _sqlite_rebuild_portfolio_snapshots(connection) -> None:
    connection.execute(
        text(
            """
            CREATE TABLE portfolio_snapshots_new (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                total_value FLOAT,
                total_invested FLOAT,
                pnl FLOAT,
                date DATE,
                CONSTRAINT uq_portfolio_snapshots_user_date UNIQUE (user_id, date)
            )
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO portfolio_snapshots_new (id, user_id, total_value, total_invested, pnl, date)
            SELECT id, user_id, total_value, total_invested, pnl, date
            FROM portfolio_snapshots
            """
        )
    )
    connection.execute(text("DROP TABLE portfolio_snapshots"))
    connection.execute(text("ALTER TABLE portfolio_snapshots_new RENAME TO portfolio_snapshots"))
    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_portfolio_snapshots_id ON portfolio_snapshots (id)"))
    connection.execute(
        text("CREATE INDEX IF NOT EXISTS ix_portfolio_snapshots_user_id ON portfolio_snapshots (user_id)")
    )


def _sqlite_rebuild_imported_portfolio_snapshots(connection) -> None:
    connection.execute(
        text(
            """
            CREATE TABLE imported_portfolio_snapshots_new (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                total_value FLOAT,
                total_invested FLOAT,
                pnl FLOAT,
                date DATE,
                CONSTRAINT uq_imported_portfolio_snapshots_user_date UNIQUE (user_id, date)
            )
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO imported_portfolio_snapshots_new (id, user_id, total_value, total_invested, pnl, date)
            SELECT id, user_id, total_value, total_invested, pnl, date
            FROM imported_portfolio_snapshots
            """
        )
    )
    connection.execute(text("DROP TABLE imported_portfolio_snapshots"))
    connection.execute(text("ALTER TABLE imported_portfolio_snapshots_new RENAME TO imported_portfolio_snapshots"))
    connection.execute(
        text("CREATE INDEX IF NOT EXISTS ix_imported_portfolio_snapshots_id ON imported_portfolio_snapshots (id)")
    )
    connection.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_imported_portfolio_snapshots_user_id "
            "ON imported_portfolio_snapshots (user_id)"
        )
    )


def _sqlite_rebuild_sip_job_runs(connection) -> None:
    connection.execute(
        text(
            """
            CREATE TABLE sip_job_runs_new (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                run_date DATE,
                trigger VARCHAR,
                status VARCHAR,
                processed_sips INTEGER,
                skip_reason VARCHAR,
                error_message VARCHAR,
                started_at DATETIME,
                ended_at DATETIME,
                CONSTRAINT uq_sip_job_runs_user_run_date UNIQUE (user_id, run_date)
            )
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO sip_job_runs_new (
                id, user_id, run_date, trigger, status, processed_sips, skip_reason, error_message, started_at, ended_at
            )
            SELECT
                id, user_id, run_date, trigger, status, processed_sips, skip_reason, error_message, started_at, ended_at
            FROM sip_job_runs
            """
        )
    )
    connection.execute(text("DROP TABLE sip_job_runs"))
    connection.execute(text("ALTER TABLE sip_job_runs_new RENAME TO sip_job_runs"))
    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_sip_job_runs_id ON sip_job_runs (id)"))
    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_sip_job_runs_user_id ON sip_job_runs (user_id)"))
    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_sip_job_runs_run_date ON sip_job_runs (run_date)"))
    connection.execute(
        text("CREATE INDEX IF NOT EXISTS ix_sip_job_runs_started_at ON sip_job_runs (started_at)")
    )


def ensure_compatible_schema(target_engine: Engine) -> None:
    # Keep this lightweight and safe for startup: only upgrade known legacy SQLite layouts.
    if target_engine.dialect.name == "postgresql":
        user_owned_tables = [
            "holdings",
            "transactions",
            "imported_holdings",
            "imported_holding_transactions",
            "recurring_sips",
            "portfolio_snapshots",
            "imported_portfolio_snapshots",
            "sip_job_runs",
        ]
        with target_engine.begin() as connection:
            for table_name in user_owned_tables:
                connection.execute(
                    text(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS user_id INTEGER")
                )

            users = connection.execute(text("SELECT id FROM users ORDER BY id ASC")).fetchall()
            if len(users) == 1:
                default_user_id = int(users[0][0])
                for table_name in user_owned_tables:
                    connection.execute(
                        text(f"UPDATE {table_name} SET user_id = :user_id WHERE user_id IS NULL"),
                        {"user_id": default_user_id},
                    )

            connection.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM pg_constraint WHERE conname = 'uq_portfolio_snapshots_date'
                        ) THEN
                            ALTER TABLE portfolio_snapshots DROP CONSTRAINT uq_portfolio_snapshots_date;
                        END IF;
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint WHERE conname = 'uq_portfolio_snapshots_user_date'
                        ) THEN
                            ALTER TABLE portfolio_snapshots
                                ADD CONSTRAINT uq_portfolio_snapshots_user_date UNIQUE (user_id, date);
                        END IF;
                    END $$;
                    """
                )
            )
            connection.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM pg_constraint WHERE conname = 'uq_imported_portfolio_snapshots_date'
                        ) THEN
                            ALTER TABLE imported_portfolio_snapshots
                                DROP CONSTRAINT uq_imported_portfolio_snapshots_date;
                        END IF;
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint WHERE conname = 'uq_imported_portfolio_snapshots_user_date'
                        ) THEN
                            ALTER TABLE imported_portfolio_snapshots
                                ADD CONSTRAINT uq_imported_portfolio_snapshots_user_date UNIQUE (user_id, date);
                        END IF;
                    END $$;
                    """
                )
            )
            connection.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM pg_constraint WHERE conname = 'uq_sip_job_runs_run_date'
                        ) THEN
                            ALTER TABLE sip_job_runs DROP CONSTRAINT uq_sip_job_runs_run_date;
                        END IF;
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint WHERE conname = 'uq_sip_job_runs_user_run_date'
                        ) THEN
                            ALTER TABLE sip_job_runs
                                ADD CONSTRAINT uq_sip_job_runs_user_run_date UNIQUE (user_id, run_date);
                        END IF;
                    END $$;
                    """
                )
            )

            for table_name in user_owned_tables:
                connection.execute(
                    text(f"CREATE INDEX IF NOT EXISTS ix_{table_name}_user_id ON {table_name} (user_id)")
                )
        return

    if target_engine.dialect.name != "sqlite":
        return

    user_owned_tables = [
        "holdings",
        "transactions",
        "imported_holdings",
        "imported_holding_transactions",
        "recurring_sips",
        "portfolio_snapshots",
        "imported_portfolio_snapshots",
        "sip_job_runs",
    ]

    with target_engine.begin() as connection:
        default_user_id = None
        if _sqlite_table_exists(connection, "users"):
            users = connection.execute(text("SELECT id FROM users ORDER BY id ASC")).fetchall()
            if len(users) == 1:
                default_user_id = int(users[0][0])

        for table_name in user_owned_tables:
            _sqlite_backfill_user_id(connection, table_name, default_user_id)

        # Old schema had unique(date/run_date) constraints. Rebuild when detected.
        portfolio_indexes = _sqlite_unique_index_columns(connection, "portfolio_snapshots")
        if ["date"] in portfolio_indexes:
            _sqlite_rebuild_portfolio_snapshots(connection)

        imported_indexes = _sqlite_unique_index_columns(connection, "imported_portfolio_snapshots")
        if ["date"] in imported_indexes:
            _sqlite_rebuild_imported_portfolio_snapshots(connection)

        sip_run_indexes = _sqlite_unique_index_columns(connection, "sip_job_runs")
        if ["run_date"] in sip_run_indexes:
            _sqlite_rebuild_sip_job_runs(connection)
