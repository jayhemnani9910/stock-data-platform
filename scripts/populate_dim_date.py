from datetime import date, timedelta

from db_utils import batch_insert, get_db_connection

# FRED macro series reach back to 1947, and the range used to start at 1990 --
# so 1,618 of 3,081 fact_macro_data rows (53% of the table) had no dimension
# row to join to. Nothing complained, because no fact table referenced dim_date
# at all. It does now (SQL/migrations/0002), so this range has to cover every
# date any loader can produce. 2050 leaves room; the old 2035 was already close
# enough to be a live problem.
START_DATE = date(1945, 1, 1)
END_DATE = date(2050, 12, 31)


def generate_date_records(start, end):
    """Build the dim_date rows for a range, inclusive of both ends.

    Pure and parameterised so it can be tested without a database. The
    DB-writing wrapper below is the only part that needs a connection.
    """
    records = []
    current = start
    while current <= end:
        dow = current.weekday()
        records.append(
            (
                current,
                current.year,
                (current.month - 1) // 3 + 1,
                current.month,
                current.day,
                dow,
                dow >= 5,
            )
        )
        current += timedelta(days=1)
    return records


def generate_dates():
    records = generate_date_records(START_DATE, END_DATE)

    with get_db_connection() as conn:
        batch_insert(
            conn,
            "INSERT INTO dim_date (date, year, quarter, month, day_of_month, day_of_week, is_weekend) "
            "VALUES %s ON CONFLICT (date) DO NOTHING",
            records,
        )

    print(f"dim_date populated from {START_DATE} to {END_DATE} ({len(records)} rows)")


if __name__ == "__main__":
    generate_dates()
