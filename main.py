import sqlite3
import pandas as pd
from radio_operator import RadioOperator
import asyncio
import aioconsole
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from radio_operator import Base
from urllib3.exceptions import MaxRetryError
from requests.exceptions import ReadTimeout
import argparse

# Create an SQLite database engine
engine = create_engine("sqlite:///checkins.db")


def log_call_sign_orm(repeater: str, call_sign: str) -> None:
    """
    Log the call sign using SQLAlchemy ORM.
    
    Args:
        repeater (str): The repeater being used.
        call_sign (str): The call sign to log.
    """
    with Session(engine) as session:
        try:
            operator = RadioOperator(call_sign, repeater)
            session.add(operator)
            session.commit()
        except ValueError as e:
            print(str(e))


def log_call_sign_pd(repeater: str, call_sign: str) -> None:
    """
    Log the call sign using Pandas and SQLite directly.
    
    Args:
        repeater (str): The repeater being used.
        call_sign (str): The call sign to log.
    """
    print("Task started")
    with sqlite3.connect("checkins.db") as db:
        try:
            operator = RadioOperator(call_sign, repeater)
            user_info = operator.operator_info()
            user_df = pd.DataFrame(user_info, index=[0])
            user_df.to_sql("checkins", db, if_exists="append", index=False)
        except ValueError as e:
            print(str(e))
        except MaxRetryError as e:
            print(f"Max Retries Error: {str(e)}")
        except ConnectionError as e:
            print(f"ConnectionError: {str(e)}")
        except TimeoutError as e:
            print(f"TimeoutError: {str(e)}")
        except ReadTimeout as e:
            print(f"ReadTimeout: {str(e)}")
    print("Task complete")


async def main(default_repeater: str = "VE7RVF", accept_default: bool = False):
    """
    Main function to handle user input and logging call signs.
    
    Args:
        default_repeater (str): The default repeater to use.
        accept_default (bool): Whether to accept the default repeater.
    """
    loop = asyncio.get_running_loop()
    repeater = default_repeater
    if not accept_default:
        repeater = await aioconsole.ainput(f"Repeater (default: {default_repeater}): ") or default_repeater
    print(f"Using repeater: {repeater}")

    while True:
        call_sign = await aioconsole.ainput("Callsign: ")
        call_sign = call_sign.strip().upper()
        if not call_sign:
            continue
        loop.run_in_executor(None, log_call_sign_orm, repeater, call_sign)


if __name__ == '__main__':
    # Argument parser for command line options
    parser = argparse.ArgumentParser(
        prog='Net Control - Check-ins',
        description='Program that logs the check-ins.',
        epilog='This program looks up Canadian and American call signs automatically'
    )
    parser.add_argument(
        '-d', '--accept-defaults',
        help="Accept default(s) (e.g. VE7RVF repeater)",
        action=argparse.BooleanOptionalAction
    )
    args = parser.parse_args()

    # Create all tables in the database
    Base.metadata.create_all(engine)

    # Start the asyncio event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    main_task = loop.create_task(main(accept_default=args.accept_defaults))
    try:
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        print("Cancelled")
    except EOFError:
        print("Cancelled during input")
    finally:
        pending_tasks = asyncio.all_tasks(loop=loop)
        for task in pending_tasks:
            task.cancel()
        loop.close()
