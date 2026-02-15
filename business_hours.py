"""
UK Business Hours Enforcement Module

Enforces strict UK business hours (06:00-21:00 ALL DAYS, Europe/London timezone)
for all outbound emails.

Business Hours:
- Monday to Sunday: 6 AM to 9 PM UK time (Europe/London)

All timestamps are stored in UTC but business hour calculations are performed in Europe/London.
DST (BST/GMT) transitions are handled automatically by pytz.
"""

from datetime import datetime, timedelta, UTC
import pytz
import logging

logger = logging.getLogger(__name__)

# UK timezone
UK_TZ = pytz.timezone('Europe/London')

# Business hours constraints
BUSINESS_HOURS_START = 6  # 06:00 UTC+0/+1
BUSINESS_HOURS_END = 21  # 21:00 UTC+0/+1
BUSINESS_DAYS = [0, 1, 2, 3, 4, 5, 6]  # Monday=0 to Sunday=6 (ALL DAYS)


def next_allowed_uk_business_time(utc_timestamp: datetime) -> datetime:
    """
    Calculate the next valid UK business send time.
    
    Business rules:
    - Monday to Sunday (ALL DAYS), 06:00 to 21:00 UK time (Europe/London)
    - Before 06:00 on any day: Same day at 06:00
    - Within 06:00-21:00 on any day: Now (immediate send)
    - At or after 21:00 on any day: Next day at 06:00
    
    Args:
        utc_timestamp: Current UTC timestamp (timezone-naive or timezone-aware)
        
    Returns:
        Next allowed UTC timestamp (timezone-naive) for sending
    """
    
    # Normalize input: ensure UTC timezone-aware
    if utc_timestamp is None:
        utc_timestamp = datetime.now(UTC)
    
    # If the timestamp is naive, assume UTC
    if utc_timestamp.tzinfo is None:
        utc_timestamp = utc_timestamp.replace(tzinfo=UTC)
    elif utc_timestamp.tzinfo != UTC:
        # Convert to UTC if it's a different timezone
        utc_timestamp = utc_timestamp.astimezone(UTC)
    
    # Convert to UK timezone
    uk_time = utc_timestamp.astimezone(UK_TZ)
    
    logger.debug(f"[BUSINESS_HOURS] Converting {utc_timestamp} UTC -> {uk_time} UK time")
    
    weekday = uk_time.weekday()
    hour = uk_time.hour
    
    # Helper function to set time in UK timezone
    def set_time_uk(dt, h, m=0, s=0):
        """Set time while preserving the date and UK timezone"""
        return dt.replace(hour=h, minute=m, second=s, microsecond=0)
    
    # Helper function to move to next day
    def next_day_uk(dt):
        """Move to next day at same time in UK timezone"""
        return (dt + timedelta(days=1)).replace(hour=dt.hour, minute=dt.minute, second=dt.second, microsecond=0)
    
    # Saturday=5 or Sunday=6: move to Monday 06:00
    if weekday >= 5:
        # Days until next Monday
        days_until_monday = (7 - weekday) % 7
        if days_until_monday == 0:  # It's already Monday
            days_until_monday = 0
        else:
            days_until_monday = 7 - weekday
        
        next_send = uk_time + timedelta(days=days_until_monday)
        next_send = set_time_uk(next_send, BUSINESS_HOURS_START)  # 06:00
        logger.debug(f"[BUSINESS_HOURS] Weekend detected (weekday={weekday}). Moving to next Monday 06:00: {next_send}")
    
    # Weekday (Mon-Fri)
    elif weekday in BUSINESS_DAYS:
        # Before 06:00: same day at 06:00
        if hour < BUSINESS_HOURS_START:
            next_send = set_time_uk(uk_time, BUSINESS_HOURS_START)
            logger.debug(f"[BUSINESS_HOURS] Weekday before 06:00. Scheduling for same day 06:00: {next_send}")
        
        # Between 06:00 and 21:00: immediate send
        elif BUSINESS_HOURS_START <= hour < BUSINESS_HOURS_END:
            next_send = uk_time
            logger.debug(f"[BUSINESS_HOURS] Within business hours (06:00-21:00). Immediate send: {next_send}")
        
        # At or after 21:00: next weekday at 06:00
        else:  # hour >= BUSINESS_HOURS_END
            next_send = uk_time + timedelta(days=1)
            next_send = set_time_uk(next_send, BUSINESS_HOURS_START)  # 06:00
            
            # If next day is weekend, skip to next Monday
            next_weekday = next_send.weekday()
            if next_weekday >= 5:
                days_to_monday = (7 - next_weekday) % 7
                if days_to_monday == 0:
                    days_to_monday = 1
                next_send = next_send + timedelta(days=days_to_monday)
                next_send = set_time_uk(next_send, BUSINESS_HOURS_START)
            
            logger.debug(f"[BUSINESS_HOURS] Weekday after 21:00. Scheduling for next weekday 06:00: {next_send}")
    
    else:
        # Defensive: shouldn't reach here, but handle gracefully
        logger.error(f"[BUSINESS_HOURS] Unexpected weekday {weekday}, assuming next weekday at 06:00")
        next_send = uk_time + timedelta(days=1)
        next_send = set_time_uk(next_send, BUSINESS_HOURS_START)
    
    # Convert back to UTC
    next_send_utc = next_send.astimezone(UTC)
    
    # Return as timezone-naive UTC for database storage
    result = next_send_utc.replace(tzinfo=None)
    logger.debug(f"[BUSINESS_HOURS] Final result: {result} (UTC, timezone-naive)")
    
    return result


def is_business_hours(utc_timestamp: datetime) -> bool:
    """
    Check if a given UTC timestamp falls within UK business hours.
    
    Args:
        utc_timestamp: UTC timestamp to check
        
    Returns:
        True if within business hours, False otherwise
    """
    if utc_timestamp is None:
        return False
    
    # Normalize input
    if utc_timestamp.tzinfo is None:
        utc_timestamp = utc_timestamp.replace(tzinfo=UTC)
    elif utc_timestamp.tzinfo != UTC:
        utc_timestamp = utc_timestamp.astimezone(UTC)
    
    # Convert to UK time
    uk_time = utc_timestamp.astimezone(UK_TZ)
    
    weekday = uk_time.weekday()
    hour = uk_time.hour
    
    # Must be Monday-Friday, 06:00-21:00
    is_weekday = weekday in BUSINESS_DAYS
    is_hour_ok = BUSINESS_HOURS_START <= hour < BUSINESS_HOURS_END
    
    return is_weekday and is_hour_ok


def get_hours_until_business_hours(utc_timestamp: datetime) -> float:
    """
    Calculate hours until the next business hours window.
    
    Args:
        utc_timestamp: Current UTC timestamp
        
    Returns:
        Number of hours (float) until next business hours start
    """
    if utc_timestamp is None:
        utc_timestamp = datetime.now(UTC)
    
    next_business = next_allowed_uk_business_time(utc_timestamp)
    
    # Calculate difference
    if utc_timestamp.tzinfo is None:
        utc_timestamp = utc_timestamp.replace(tzinfo=UTC)
    elif utc_timestamp.tzinfo != UTC:
        utc_timestamp = utc_timestamp.astimezone(UTC)
    
    delta = next_business.replace(tzinfo=UTC) - utc_timestamp
    hours = delta.total_seconds() / 3600.0
    
    return max(0, hours)


if __name__ == '__main__':
    """Test the business hours calculation"""
    import sys
    
    logging.basicConfig(level=logging.DEBUG)
    
    # Test cases
    test_cases = [
        ("Monday 02:00 UTC", datetime(2024, 1, 1, 2, 0, 0, tzinfo=UTC)),  # Before hours - move to 06:00
        ("Monday 06:00 UTC", datetime(2024, 1, 1, 6, 0, 0, tzinfo=UTC)),  # Start of hours - immediate
        ("Monday 12:00 UTC", datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)),  # Within hours - immediate
        ("Monday 20:00 UTC", datetime(2024, 1, 1, 20, 0, 0, tzinfo=UTC)),  # Within hours - immediate
        ("Monday 21:00 UTC", datetime(2024, 1, 1, 21, 0, 0, tzinfo=UTC)),  # After hours - next day 06:00
        ("Saturday 12:00 UTC", datetime(2024, 1, 6, 12, 0, 0, tzinfo=UTC)),  # Weekend - next Monday 06:00
    ]
    
    for name, ts in test_cases:
        result = next_allowed_uk_business_time(ts)
        is_biz = is_business_hours(ts)
        hours_until = get_hours_until_business_hours(ts)
        print(f"\n{name}:")
        print(f"  Input (UTC): {ts}")
        print(f"  Next allowed (UTC): {result}")
        print(f"  Is business hours: {is_biz}")
        print(f"  Hours until: {hours_until:.1f}h")
