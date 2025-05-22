import re
import time
import smtplib
from datetime import datetime
from functools import wraps
from email.message import EmailMessage

import config


def parse_phone_number(number: str) -> str:
    """
    Normalizes a UK phone number to local format starting with '0'.

    This function removes all non-digit characters from the input,
    strips the UK country code prefix ('44') if present, and ensures
    the resulting number starts with a leading zero.

    Args:
        number (str): The raw phone number (e.g. '+44 7802 312241').

    Returns:
        str: Normalized UK phone number (e.g. '07802312241').
    """
    number = ''.join(filter(str.isdigit, number))
    if number.startswith("44"):
        number = number[2:]
    if not number.startswith("0"):
        number = "0" + number
    return number


def parse_long_form_date(date_str: str) -> str:
    """
    Converts a string like 'Sunday 25th January 2026' into '25/01/2026'.

    Args:
        date_str (str): Date in the format 'DayOfWeek DDth Month YYYY'

    Returns:
        str: Date in 'dd/mm/yyyy' format.
    """
    # Remove the day of the week and ordinal suffix
    # E.g., 'Sunday 25th January 2026' -> '25 January 2026'
    cleaned = re.sub(r'\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b', '', date_str)
    cleaned = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', cleaned).strip()

    # Parse to datetime object
    dt = datetime.strptime(cleaned, "%d %B %Y")

    # Format to dd/mm/yyyy
    return dt.strftime("%d/%m/%Y")

def send_booking_alert(sender_email, sender_password, recipient_emails, message_body, logger=None, debug=False, log_only=False):
    """
    Sends an HTML-formatted email with the subject "Booking alert" to multiple recipients.

    Parameters:
    - sender_email (str): The sender's email address.
    - sender_password (str): The password or app-specific password for the sender's email.
    - recipient_emails (list of str): A list of recipient email addresses.

    Returns:
    None. Prints success or failure message to the console.
    """
    msg = EmailMessage()
    msg['Subject'] = 'Booking alert'
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipient_emails) if not debug else recipient_emails[0]

    msg.set_content("This email requires an HTML-compatible email client.")
    msg.add_alternative(message_body, subtype='html')

    if log_only:
        logger.info("Email not sent in log-only mode")
        logger.info(message_body)
        return
    elif debug:
        logger.info(f"Email only sent to {recipient_emails[0]} in debug mode")
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(sender_email, sender_password)
            smtp.send_message(msg)
        logger.info("Email sent successfully.")
        logger.info(message_body)
    except Exception as e:
        logger.critical(f"Failed to send email: {e}")
    return


def retry(max_retries=3, delay=2, logger=None):
    """
    A decorator that retries a function call if an exception occurs.

    Parameters:
    - max_retries (int): Maximum number of retry attempts. Default is 3.
    - delay (int or float): Delay (in seconds) between retry attempts. Default is 2.
    - logger (Logger): Optional logger for logging retry attempts.
    - alert_email (dict): Dictionary with email info:
        {
            "to": ["recipient@example.com"],
            "subject": "Function Failure Alert"
        }

    Returns:
    - The result of the decorated function if successful.
    - Sends an email and raises the last exception if all retries fail.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if logger:
                        logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(delay)
                    else:
                        message = f"Failed to update bookings after {max_retries} attempts.\n\nError: {e}"
                        send_booking_alert(
                            sender_email=config.GMAIL_ACCOUNT,
                            sender_password=config.GMAIL_PASSWORD,
                            recipient_emails=config.LIVE_EMAILS,
                            message_body=message,
                            logger=logger,
                            debug=True)
                        raise
        return wrapper
    return decorator
