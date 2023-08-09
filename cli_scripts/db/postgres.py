import psycopg2
import argparse


def main(query: str):
    try:
        connection = psycopg2.connect(
            user="postgres", password="postgres", host="localhost", database="charts"
        )
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("query_file", type=str, help="Path to query file")
    args = parser.parse_args()

    with open(args.query_file, "r") as f:
        query = f.read()

    main(query)
