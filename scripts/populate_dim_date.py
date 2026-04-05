from datetime import date, timedelta

from db_utils import batch_insert, get_db_connection

START_DATE = date(1990, 1, 1)
END_DATE = date(2035, 12, 31)


def generate_dates():
    records = []
    current = START_DATE
    while current <= END_DATE:
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

    with get_db_connection() as conn:
        batch_insert(
            conn,
            "INSERT INTO dim_date (date, year, quarter, month, day_of_month, day_of_week, is_weekend) "
            "VALUES %s ON CONFLICT (date) DO NOTHING",
            records,
        )

    print(f"dim_date populated from {START_DATE} to {END_DATE}")


if __name__ == "__main__":
    generate_dates()
