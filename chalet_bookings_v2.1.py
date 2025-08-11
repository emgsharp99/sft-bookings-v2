import argparse
import config
import copy
from datetime import datetime
from flask import Flask, request
from functions import parse_phone_number, parse_long_form_date, send_booking_alert
import gspread
import logging
import numpy as np
import pandas as pd
from pathlib import Path
import sys
from Testing import test_data
from typing import Optional

LOG_FILE_PATH = Path(__file__).parent / "bookings.log"
LOGGER = logging.getLogger(__name__)
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

app = Flask(__name__)

class Booking:
    def __init__(self):
        self.leader_details: Optional[dict] = None
        self.guests: Optional[dict] = None
        self.how_heard: Optional[str] = None
        self.dietaries: Optional[str] = None
        self.kids_meals: Optional[str] = None

    def log(self, logger) -> None:
        logger.info(self.__dict__)

    def add_to_master(self, client, logger=None) -> None:
        """
        Adds the booking and guest data to the master Google Sheet.
        """
        sheet_id = config.CHALET_MASTER_SHEET
        sheet = client.open_by_key(sheet_id).get_worksheet(0)
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
        booking_ref = leader["family_name"]
        logger.info(f"Cells updated successfully for {booking_ref}")


@app.route('/googleform', methods=['POST'])
def google_form_webhook():
    data = request.get_json()
    filtered_data = filter_data(data)
    LOGGER.info("Webhook triggered: response form submitted.")
    LOGGER.info(f"Non-null results:\n {filtered_data}")

    booking = parse_booking(filtered_data)
    booking.add_to_master(client = gspread.service_account(), logger=LOGGER)

    html_email_content = f"""
        <html>
            <body>
                <p><strong>2025/2026 BOOKINGS:</strong></p>
                <p>There was a new chalet booking, the master sheet has been updated.</p>
                <p><em>This alert was generated at {datetime.now()}.</em></p>
            </body>
        </html>
        """
    send_booking_alert(config.GMAIL_ACCOUNT, config.GMAIL_PASSWORD, config.LIVE_EMAILS, html_email_content, LOGGER)
    return "OK", 200

def filter_data(data: dict) -> dict:
    return {k: v[0] for k, v in data.items() if any(x.strip() for x in v)}

def parse_booking(data: dict) -> Booking:
    booking = Booking()
    booked_on = pd.to_datetime(data["Timestamp"]).strftime("%d/%m/%Y")
    family_name = data["Family name"]
    chalet = data["Which chalet?"]
    start_date = parse_long_form_date(data[f"Start of stay in chalet {chalet}"])
    end_date = parse_long_form_date(data[f"End of stay in chalet {chalet}"])

    booking.leader_details = {
        "first_name": data["First name"],
        "family_name": family_name,
        "full_name": f"{data['First name']} {family_name}",
        "email": data["Email address"],
        "dob": data["Date of birth"],
        "add1": data["Address 1"],
        "add2": data["Address 2"],
        "add3": data.get("Address 3") or None,
        "town": data["Town"],
        "postcode": data["Postcode"],
        "phone": parse_phone_number(data["Contact telephone number"]),
        "room": data["Which room will you be staying in?"],
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
        first_name = data.get(f"First name {person_label}")
        if not first_name:
            break
        booking.guests[person_number] = {
            "first_name": first_name,
            "family_name": data.get(f"Family name {person_label}", ""),
            "full_name": f"{first_name} {data.get(f'Family name {person_label}', '')}",
            "email": data.get(f"Email address {person_label}"),
            "dob": data.get(f"Date of birth {person_label}"),
            "room": data.get(f"Which room will this person be in? {person_label}"),
            "group": family_name,
            "chalet": chalet,
            "start_date": start_date,
            "end_date": end_date,
            "booked_on": booked_on
        }
        person_number += 1

    booking.dietaries = data.get("What are the dietary requirements?")
    booking.kids_meals = data.get("How many children's meals do you require?")
    booking.how_heard = data.get("Please tell us how you heard about us. It would be really helpful!")
    return booking

def main():
    started = datetime.now()
    logging.basicConfig(level=logging.INFO)
    LOGGER.info(f"Started @ {started}")

    parser = argparse.ArgumentParser(description="Chalet Booking Processor")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        LOGGER.info("*** DEBUG MODE ENABLED - using test data ***")
        data = filter_data(test_data.data)
        booking = parse_booking(data)
        booking.log(LOGGER)
    else:
        app.run(host='0.0.0.0', port=3000)

if __name__ == '__main__':
    main()