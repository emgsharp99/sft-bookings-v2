import argparse
import copy
import hashlib
import json
import logging
import pprint
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

import gspread
import numpy as np
import pandas as pd

from functions import parse_phone_number, parse_long_form_date
import config

# Constants
HASH_DIRECTORY = Path(__file__).parent / 'Data' / 'hash.json'
BOOKINGS_DIRECTORY = Path(__file__).parent / 'Data' / 'bookings.json'
LOG_FILE_PATH = Path(__file__).parent / "bookings.log"

# Settings
pd.set_option('future.no_silent_downcasting', True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler(sys.stdout)  # Optional: also logs to console
    ]
)

MASTER_COLUMNS_MAPPING = {
    "first_name": 3,
    "family_name": 4,
    "full_name": 5,
    "dob": 6,
    "add1": 7,
    "add2": 8,
    "add3": 9,
    "town": 10,
    "postcode": 11,
    "email": 12,
    "phone": 13,
    "group": 14,
    "start_date": 16,
    "end_date": 17,
    "chalet": 19,
    "room": 20,
    "booked_on": 21,
    "kids_meals": 41,
    "dietaries": 42,
    "how_heard": 43
}


class Booking:
    def __init__(self):
        self.booking_time: Optional[str] = None
        self.leader_details: Optional[dict] = None
        self.guests: Optional[dict] = None
        self.start_date: Optional[str] = None
        self.end_date: Optional[str] = None
        self.how_heard: Optional[str] = None
        self.dietaries: Optional[str] = None
        self.kids_meals: Optional[str] = None

    def pretty_print(self) -> None:
        pprint.pprint(self.__dict__)

    def dump_to_json(self, path: Path) -> None:
        bookings = {}
        if path.exists():
            with open(path, "r") as f:
                bookings = json.load(f)
        new_id = str(max(map(int, bookings.keys()), default=0) + 1)
        bookings[new_id] = self.__dict__
        with open(path, "w") as f:
            json.dump(bookings, f, indent=2)

    def add_to_master(self, client) -> None:
        """
        Adds the booking and guest data to the master Google Sheet.
        """
        sheet = client.open_by_key(config.TEST_MASTER_SHEET).get_worksheet(0)
        master = pd.DataFrame(sheet.get_all_values())
        master.columns = master.iloc[0]
        master = master[1:]
        master.replace('', np.nan, inplace=True)
        master.dropna(subset=['Identifier'], inplace=True)

        def add_cell(cells, row, col, value):
            cells.append(gspread.Cell(row=row, col=col, value=value))

        def map_fields(data, row, cells):
            for key, value in data.items():
                col = MASTER_COLUMNS_MAPPING.get(key)
                if col:
                    add_cell(cells, row, col, value)

        start_row = len(master) + 2
        cells = []

        booking_data = copy.deepcopy(self.__dict__)
        guests = booking_data.pop('guests', {})
        leader = booking_data.pop('leader_details', {})

        add_cell(cells, start_row, 2, start_row - 1)
        add_cell(cells, start_row, 15, True)

        map_fields(booking_data, start_row, cells)
        map_fields(leader, start_row, cells)

        for i, guest in enumerate(guests.values(), start=1):
            row = start_row + i
            add_cell(cells, row, 2, row - 1)
            map_fields(guest, row, cells)

        sheet.update_cells(cells, value_input_option='USER_ENTERED')


def check_sheet_changed(df: pd.DataFrame, logger=None) -> bool:
    """
    Compares the hash of the current sheet to the stored hash to detect updates.
    """
    df_bytes = pd.util.hash_pandas_object(df, index=True).values.tobytes()
    current_hash = hashlib.sha256(df_bytes).hexdigest()

    previous_hash = None
    previous_datetime = datetime(1970, 1, 1)

    if HASH_DIRECTORY.exists():
        with open(HASH_DIRECTORY, "r") as f:
            data = json.load(f)
            previous_hash = data.get("hash")
            previous_datetime = pd.to_datetime(data.get("datetime"))

    if previous_hash == current_hash:
        if logger:
            logger.info("No new responses.")
        return False

    if logger:
        logger.info("New responses found.")
    globals()["current_hash"] = current_hash  # update global for later saving
    globals()["previous_datetime"] = previous_datetime
    return True


def parse_row(row: pd.Series) -> Booking:
    booking = Booking()
    booked_on = pd.to_datetime(row["Timestamp"]).strftime("%d/%m/%Y")
    family_name = row["Family name"]
    chalet = row["Which chalet?"]
    start_date = parse_long_form_date(row[f"Start of stay in chalet {chalet}"])
    end_date = parse_long_form_date(row[f"End of stay in chalet {chalet}"])

    booking.leader_details = {
        "first_name": row["First name"],
        "family_name": family_name,
        "full_name": f"{row['First name']} {family_name}",
        "email": row["Email address"],
        "dob": row["Date of birth"],
        "add1": row["Address 1"],
        "add2": row["Address 2"],
        "add3": row.get("Address 3") or None,
        "town": row["Town"],
        "postcode": row["Postcode"],
        "phone": parse_phone_number(row["Contact telephone number"]),
        "room": row["Which room will you be staying in?"],
        "group": family_name,
        "chalet": chalet,
        "start_date": start_date,
        "end_date": end_date,
        "booked_on": booked_on
    }

    booking.guests = {}
    person_number = 2
    while True:
        person_label = f"(person {person_number})"
        first_name = row.get(f"First name {person_label}")
        if not first_name:
            break
        booking.guests[person_number] = {
            "first_name": first_name,
            "family_name": row.get(f"Family name {person_label}", ""),
            "full_name": f"{first_name} {row.get(f'Family name {person_label}', '')}",
            "email": row.get(f"Email address {person_label}"),
            "dob": row.get(f"Date of birth {person_label}"),
            "room": row.get(f"Which room will this person be in? {person_label}"),
            "group": family_name,
            "chalet": chalet,
            "start_date": start_date,
            "end_date": end_date,
            "booked_on": booked_on
        }
        person_number += 1

    booking.dietaries = row.get("What are the dietary requirements?")
    booking.kids_meals = row.get("How many children's meals do you require?")
    booking.how_heard = row.get("Please tell us how you heard about us. It would be really helpful!")
    return booking


def process_responses(client, logger=None):
    try:
        sheet = client.open_by_key(config.CHALET_RESPONSE_SHEET).get_worksheet(0)
        responses = pd.DataFrame(sheet.get_all_values())
    except Exception:
        logger.critical("Failed to fetch sheet:\n" + traceback.format_exc())
        sys.exit(1)

    if responses.empty:
        logger.info("No responses yet.")
        sys.exit(1)

    if not check_sheet_changed(responses, logger):
        sys.exit(1)

    responses.columns = responses.iloc[0]
    responses = responses.drop(index=0).reset_index(drop=True)
    responses['Timestamp'] = pd.to_datetime(responses['Timestamp'], dayfirst=True)
    new_responses = responses[responses['Timestamp'] > previous_datetime]

    logger.info(f"{len(new_responses)} new responses found.")
    for _, row in new_responses.iterrows():
        cleaned_row = row.replace(r'^\s*$', pd.NA, regex=True).dropna()
        booking = parse_row(cleaned_row)
        booking.dump_to_json(BOOKINGS_DIRECTORY)
        booking.add_to_master(client)


def main(started=datetime.now()):
    logger = logging.getLogger("bookings")
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Chalet Booking Processor")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.info("*** DEBUG MODE ENABLED ***")
        for path in [HASH_DIRECTORY, BOOKINGS_DIRECTORY]:
            if path.exists():
                path.unlink()

    process_responses(client=gspread.service_account(), logger=logger)

    with open(HASH_DIRECTORY, 'w') as f:
        json.dump({"datetime": str(started), "hash": current_hash}, f, indent=2)


if __name__ == "__main__":
    main()
