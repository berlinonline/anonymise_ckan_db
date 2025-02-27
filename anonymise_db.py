import argparse
import json
import logging
import os
import sys
from getpass import getpass

import psycopg2
from faker import Faker
from slugify import slugify

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


# Configuration
DEFAULT_DB_NAME = "ckan"
DEFAULT_DB_USER = "ckan"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = "5432"
DEFAULT_EXCLUDE_FILE = "exclude.json"

def modify_and_dump(dbname: str, user: str, password: str, host: str, port: str, exclude: list=[]):
    """Connects to PostgreSQL, modifies data in user table."""
    fake = Faker("de_DE")
    LOG.info(" connecting to DB")
    conn = psycopg2.connect(
        dbname=dbname, user=user, password=password, host=host, port=port
    )
    conn.autocommit = False  # Ensure we can roll back changes
    cursor = conn.cursor()

    try:
        # Modify users in-memory
        cursor.execute('SELECT id,name FROM "user";')
        users = cursor.fetchall()
        
        names = []

        for (user_id,name) in users:
            if name in exclude:
                LOG.info(f" excluding {user_id} ({name})")
                continue
            LOG.info(f" modifying {user_id} ({name})")
            unique = False
            while not unique:
                firstname = fake.first_name()
                lastname = fake.last_name()
                fullname = f"{firstname} {lastname}"
                LOG.info(f"   new name: {fullname}")
                if fullname not in names:
                    LOG.info("   that's unique!")
                    names.append(fullname)
                    unique = True
                else:
                    LOG.info("   I've seen that name before!")

            name = slugify(fullname, separator="_")
            email_name = slugify(fullname, separator=".")
            email = f"{email_name}@example.com"

            LOG.info(f"   Updating with info: {fullname}: {name}, {email}")

            cursor.execute(
                f'UPDATE "user" SET name = \'{name}\', fullname = \'{fullname}\', email = \'{email}\', about = \'\', image_url = \'\' WHERE id = \'{user_id}\';'
            )

        LOG.info(" committing changes")
        conn.commit()
        LOG.info(" done")

    except Exception as e:
        conn.rollback()  # Ensure rollback on any error
        print(f"Error: {e}")
    finally:
        LOG.info(" closing everything")
        cursor.close()
        conn.close()
        LOG.info(" done")

parser = argparse.ArgumentParser(
    description="""Connect to a CKAN DB in Postgres and replace `fullname`, `name` and `email`
    in the `user` table with random names.""")

parser.add_argument('--db',
                    help=f"Name of the CKAN database in Postgres. Default: {DEFAULT_DB_NAME}",
                    default=DEFAULT_DB_NAME,
                    )
parser.add_argument('--user',
                    help=f"Username to access the database. Default: {DEFAULT_DB_USER}",
                    default=DEFAULT_DB_USER,
                    )
parser.add_argument('--host',
                    help=f"Postgres database host. Default: {DEFAULT_DB_HOST}.",
                    default=DEFAULT_DB_HOST,
                    )
parser.add_argument('--port',
                    help=f"Postgres database port. Default: {DEFAULT_DB_PORT}.",
                    default=DEFAULT_DB_PORT,
                    )
parser.add_argument('--exclude',
                    help=f"Path to json file with list of usernames to be excluded. Default: {DEFAULT_EXCLUDE_FILE}.",
                    default=DEFAULT_EXCLUDE_FILE,
                    )

args = parser.parse_args()

password = getpass(f"Password for user '{args.user}': ")

exclude_path = args.exclude
exclude = []
if os.path.isfile(exclude_path):
    with open(exclude_path) as f:
        exclude = json.load(f)
else:
    LOG.error(f" cannot open exclude file: {exclude_path}")
    sys.exit()


modify_and_dump(
    dbname=args.db,
    user=args.user,
    password=password,
    host=args.host,
    port=args.port,
    exclude=exclude,
)
