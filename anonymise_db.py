import logging
import subprocess
import argparse

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
DEFAULT_OUTPUT_DUMP_FILE = "anonymised.dump"

def modify_and_dump(dbname: str, user: str, password: str, host: str, port: str, dumpfile: str):
    """Connects to PostgreSQL, modifies data in a transaction, dumps, and rolls back. """
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
                f'UPDATE "user" SET name = \'{name}\', fullname = \'{fullname}\', email = \'{email}\' WHERE id = \'{user_id}\';'
            )

        # Dump the modified version
        LOG.info(f" writing dump to {dumpfile}")
        subprocess.run(
            f'PGPASSWORD={password} pg_dump -U {user} -h {host} -p {port} -d {dbname} -F c -f {dumpfile}',
            shell=True,
            check=True
        )

        LOG.info(" done")

        # Roll back to keep original data intact
        LOG.info(" rolling back db")
        conn.rollback()
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
parser.add_argument('--dumpfile',
                    help=f"Filename of the output dump. Default: {DEFAULT_OUTPUT_DUMP_FILE}.",
                    default=DEFAULT_OUTPUT_DUMP_FILE,
                    )
requiredNamed = parser.add_argument_group('required named arguments')
requiredNamed.add_argument('--pw', help='Password to access the database', required=True)

args = parser.parse_args()

modify_and_dump(
    dbname=args.db,
    user=args.user,
    password=args.pw,
    host=args.host,
    port=args.port,
    dumpfile=args.dumpfile
)