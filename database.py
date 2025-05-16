import sqlite3
import os
import logging
from datetime import datetime
import json
from typing import List, Dict, Any, Optional

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Database setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'blood_bot.db')


def create_tables():
    """Create necessary tables if they don't exist."""
    logger.info("Creating and updating database tables if needed...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Donors table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS donors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        telegram_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        age TEXT NOT NULL,
        phone TEXT NOT NULL,
        gender TEXT NOT NULL,
        blood_group TEXT NOT NULL,
        division TEXT NOT NULL,
        district TEXT NOT NULL,
        area TEXT NOT NULL,
        registration_date TEXT NOT NULL
    )
    ''')

    # Blood requests table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        telegram_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        age TEXT NOT NULL,
        hospital_name TEXT NOT NULL,
        hospital_address TEXT NOT NULL,
        area TEXT NOT NULL,
        division TEXT NOT NULL,
        district TEXT NOT NULL,
        urgency TEXT NOT NULL,
        phone TEXT NOT NULL,
        blood_group TEXT NOT NULL,
        request_date TEXT NOT NULL,
        status TEXT NOT NULL,
        donors_notified TEXT DEFAULT '[]',
        donors_accepted TEXT DEFAULT '[]',
        donors_declined TEXT DEFAULT '[]'
    )
    ''')

    # Chat groups table for managing donor-requester communications
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chat_groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT NOT NULL,
        donor_id TEXT NOT NULL,
        chat_id INTEGER NOT NULL,
        invite_link TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    ''')

    conn.commit()
    conn.close()
    logger.info("Database tables created/checked successfully")


# Ensure tables are created
create_tables()


# Donor functions
def save_donor(donor_data: Dict[str, Any]) -> str:
    """Save a new donor to the database."""
    logger.info(f"Saving new donor: {donor_data.get('name', 'Unknown')}, Blood Group: {donor_data.get('blood_group', 'Unknown')}")

    # Ensure registration_date is included
    if 'registration_date' not in donor_data:
        donor_data['registration_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get column names from the donors table
    cursor.execute('PRAGMA table_info(donors)')
    columns = [column[1] for column in cursor.fetchall()]

    # Build the insert query dynamically based on available columns
    valid_columns = []
    valid_values = []
    placeholders = []

    for key, value in donor_data.items():
        if key in columns:
            valid_columns.append(key)
            valid_values.append(value)
            placeholders.append('?')

    # Build the SQL query
    columns_str = ', '.join(valid_columns)
    placeholders_str = ', '.join(placeholders)

    query = f"INSERT INTO donors ({columns_str}) VALUES ({placeholders_str})"

    try:
        cursor.execute(query, valid_values)
        donor_id = cursor.lastrowid
        conn.commit()
        conn.close()

        logger.info(f"Donor saved successfully with ID: {donor_id}")
        return str(donor_id)
    except Exception as e:
        logger.error(f"Error saving donor: {e}")
        conn.close()
        raise


def get_donor_by_id(donor_id: str) -> Optional[Dict[str, Any]]:
    """Get donor information by ID."""
    logger.info(f"Getting donor by ID: {donor_id}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM donors WHERE id = ?', (donor_id,))
    result = cursor.fetchone()

    conn.close()

    if result:
        logger.info(f"Donor found: {result['name']}")
        return dict(result)

    logger.warning(f"Donor not found with ID: {donor_id}")
    return None


def get_donor_by_telegram_id(telegram_id: int) -> Optional[Dict[str, Any]]:
    """Get donor information by Telegram ID."""
    logger.info(f"Getting donor by Telegram ID: {telegram_id}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM donors WHERE telegram_id = ?', (telegram_id,))
    result = cursor.fetchone()

    conn.close()

    if result:
        logger.info(f"Donor found: {result['name']}")
        return dict(result)

    logger.info(f"No donor found with Telegram ID: {telegram_id}")
    return None


def get_all_donors() -> List[Dict[str, Any]]:
    """Get all registered donors."""
    logger.info("Getting all donors")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM donors ORDER BY registration_date DESC')
    results = cursor.fetchall()

    conn.close()

    donors = [dict(row) for row in results]
    logger.info(f"Retrieved {len(donors)} donors")
    return donors


def get_donors_by_location(division: str, district: str = None) -> List[Dict[str, Any]]:
    """Get donors from a specific location.

    Args:
        division (str): Division name
        district (str, optional): District name. If None, return all donors in division.

    Returns:
        List of donor dictionaries
    """
    if district:
        logger.info(f"Getting donors by location - Division: {division}, District: {district}")
    else:
        logger.info(f"Getting donors by location - Division: {division}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        if district:
            # Get donors from specific district and division
            cursor.execute(
                'SELECT * FROM donors WHERE LOWER(division) = LOWER(?) AND LOWER(district) = LOWER(?)',
                (division.lower(), district.lower())
            )
        else:
            # Get all donors from division
            cursor.execute(
                'SELECT * FROM donors WHERE LOWER(division) = LOWER(?)',
                (division.lower(),)
            )

        results = cursor.fetchall()
        conn.close()

        donors = [dict(row) for row in results]
        logger.info(f"Found {len(donors)} donors in specified location")
        return donors

    except Exception as e:
        logger.error(f"Error getting donors by location: {e}")
        conn.close()
        return []


def get_donors_by_blood_groups(blood_groups: List[str]) -> List[Dict[str, Any]]:
    """Get donors with specific blood groups."""
    if not blood_groups:
        logger.warning("No blood groups provided for donor search")
        return []

    logger.info(f"Searching for donors with blood groups: {blood_groups}")

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Create placeholders for SQL query
        placeholders = ', '.join(['?' for _ in blood_groups])

        cursor.execute(
            f'SELECT * FROM donors WHERE blood_group IN ({placeholders})',
            blood_groups
        )
        results = cursor.fetchall()

        donors = [dict(row) for row in results]
        logger.info(f"Found {len(donors)} donors matching blood groups {blood_groups}")

        conn.close()
        return donors

    except Exception as e:
        logger.error(f"Error in get_donors_by_blood_groups: {e}")
        return []


def get_donors_by_blood_groups_and_location(blood_groups: List[str], division: str, district: str = None) -> List[
    Dict[str, Any]]:
    """Get donors with specific blood groups and in a specific location.

    Args:
        blood_groups (List[str]): List of compatible blood groups
        division (str): Division name
        district (str, optional): District name. If None, match only division.

    Returns:
        List of donor dictionaries
    """
    if not blood_groups:
        logger.warning("No blood groups provided for location-based donor search")
        return []

    if district:
        logger.info(f"Searching for donors with blood groups {blood_groups} in {division}, {district}")
    else:
        logger.info(f"Searching for donors with blood groups {blood_groups} in {division}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # Create placeholders for blood groups
        placeholders = ', '.join(['?' for _ in blood_groups])

        if district:
            # Query for specific district and division
            query = f'''
                SELECT * FROM donors 
                WHERE blood_group IN ({placeholders}) 
                AND LOWER(division) = LOWER(?) 
                AND LOWER(district) = LOWER(?)
            '''
            params = blood_groups + [division.lower(), district.lower()]
        else:
            # Query for division only
            query = f'''
                SELECT * FROM donors 
                WHERE blood_group IN ({placeholders}) 
                AND LOWER(division) = LOWER(?)
            '''
            params = blood_groups + [division.lower()]

        cursor.execute(query, params)
        results = cursor.fetchall()

        conn.close()

        donors = [dict(row) for row in results]
        logger.info(f"Found {len(donors)} donors matching blood groups and location criteria")
        return donors

    except Exception as e:
        logger.error(f"Error getting donors by blood groups and location: {e}")
        conn.close()
        return []


def update_donor(donor_id: str, update_data: Dict[str, Any]) -> bool:
    """Update donor information."""
    logger.info(f"Updating donor ID {donor_id} with data: {update_data}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Build the update SQL dynamically
    update_fields = []
    values = []

    for key, value in update_data.items():
        if key not in ['id', 'telegram_id', 'registration_date']:  # Don't update these fields
            update_fields.append(f"{key} = ?")
            values.append(value)

    if not update_fields:
        logger.warning("No valid fields to update")
        conn.close()
        return False

    values.append(donor_id)  # For the WHERE clause

    cursor.execute(
        f"UPDATE donors SET {', '.join(update_fields)} WHERE id = ?",
        values
    )

    rows_affected = cursor.rowcount
    conn.commit()
    conn.close()

    if rows_affected > 0:
        logger.info(f"Successfully updated donor ID {donor_id}")
    else:
        logger.warning(f"No rows affected when updating donor ID {donor_id}")

    return rows_affected > 0


def delete_donor(donor_id: str) -> bool:
    """Delete a donor from the database."""
    logger.info(f"Deleting donor ID {donor_id}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('DELETE FROM donors WHERE id = ?', (donor_id,))

    rows_affected = cursor.rowcount
    conn.commit()
    conn.close()

    if rows_affected > 0:
        logger.info(f"Successfully deleted donor ID {donor_id}")
    else:
        logger.warning(f"No donor found with ID {donor_id} to delete")

    return rows_affected > 0


# Request functions
def save_request(request_data: Dict[str, Any]) -> str:
    """Save a new blood request to the database."""
    logger.info(f"Saving new blood request: {request_data.get('name', 'Unknown')}, Blood Group: {request_data.get('blood_group', 'Unknown')}")

    # Ensure request_date is included
    if 'request_date' not in request_data:
        request_data['request_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Ensure status is included
    if 'status' not in request_data:
        request_data['status'] = 'active'

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get column names from the requests table
    cursor.execute('PRAGMA table_info(requests)')
    columns = [column[1] for column in cursor.fetchall()]

    # Build the insert query dynamically based on available columns
    valid_columns = []
    valid_values = []
    placeholders = []

    for key, value in request_data.items():
        if key in columns:
            valid_columns.append(key)
            valid_values.append(value)
            placeholders.append('?')

    # Build the SQL query
    columns_str = ', '.join(valid_columns)
    placeholders_str = ', '.join(placeholders)

    query = f"INSERT INTO requests ({columns_str}) VALUES ({placeholders_str})"

    try:
        cursor.execute(query, valid_values)
        request_id = cursor.lastrowid
        conn.commit()
        conn.close()

        logger.info(f"Blood request saved successfully with ID: {request_id}")
        return str(request_id)
    except Exception as e:
        logger.error(f"Error saving blood request: {e}")
        conn.close()
        raise


def get_request_by_id(request_id: str) -> Optional[Dict[str, Any]]:
    """Get request information by ID."""
    logger.info(f"Getting request by ID: {request_id}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM requests WHERE id = ?', (request_id,))
    result = cursor.fetchone()

    conn.close()

    if result:
        result_dict = dict(result)
        # Parse JSON string lists to Python lists
        for key in ['donors_notified', 'donors_accepted', 'donors_declined']:
            if result_dict[key]:
                try:
                    result_dict[key] = json.loads(result_dict[key])
                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON for {key} in request {request_id}")
                    result_dict[key] = []
            else:
                result_dict[key] = []

        logger.info(f"Request found: {result_dict.get('name', 'Unknown')}, Blood Group: {result_dict.get('blood_group', 'Unknown')}")
        return result_dict

    logger.warning(f"Request not found with ID: {request_id}")
    return None


def get_active_requests() -> List[Dict[str, Any]]:
    """Get all active blood requests."""
    logger.info("Getting all active blood requests")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM requests WHERE status = "active" ORDER BY request_date DESC')
    results = cursor.fetchall()

    conn.close()

    result_list = []
    for row in results:
        row_dict = dict(row)
        # Parse JSON string lists to Python lists
        for key in ['donors_notified', 'donors_accepted', 'donors_declined']:
            if row_dict[key]:
                try:
                    row_dict[key] = json.loads(row_dict[key])
                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON for {key} in request {row_dict.get('id', 'Unknown')}")
                    row_dict[key] = []
            else:
                row_dict[key] = []
        result_list.append(row_dict)

    logger.info(f"Retrieved {len(result_list)} active requests")
    return result_list


def get_all_requests() -> List[Dict[str, Any]]:
    """Get all blood requests (active and inactive)."""
    logger.info("Getting all blood requests")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM requests ORDER BY request_date DESC')
    results = cursor.fetchall()

    conn.close()

    result_list = []
    for row in results:
        row_dict = dict(row)
        # Parse JSON string lists to Python lists
        for key in ['donors_notified', 'donors_accepted', 'donors_declined']:
            if row_dict[key]:
                try:
                    row_dict[key] = json.loads(row_dict[key])
                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON for {key} in request {row_dict.get('id', 'Unknown')}")
                    row_dict[key] = []
            else:
                row_dict[key] = []
        result_list.append(row_dict)

    logger.info(f"Retrieved {len(result_list)} total requests")
    return result_list


def get_requests_by_user(telegram_id: int) -> List[Dict[str, Any]]:
    """Get blood requests made by a specific user."""
    logger.info(f"Getting blood requests for user with Telegram ID: {telegram_id}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM requests WHERE telegram_id = ? ORDER BY request_date DESC', (telegram_id,))
    results = cursor.fetchall()

    conn.close()

    result_list = []
    for row in results:
        row_dict = dict(row)
        # Parse JSON string lists to Python lists
        for key in ['donors_notified', 'donors_accepted', 'donors_declined']:
            if row_dict[key]:
                try:
                    row_dict[key] = json.loads(row_dict[key])
                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON for {key} in request {row_dict.get('id', 'Unknown')}")
                    row_dict[key] = []
            else:
                row_dict[key] = []
        result_list.append(row_dict)

    logger.info(f"Retrieved {len(result_list)} requests for user {telegram_id}")
    return result_list


def update_request_status(request_id: str, status: str) -> bool:
    """Update the status of a blood request."""
    logger.info(f"Updating request ID {request_id} status to: {status}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        'UPDATE requests SET status = ? WHERE id = ?',
        (status, request_id)
    )

    rows_affected = cursor.rowcount
    conn.commit()
    conn.close()

    if rows_affected > 0:
        logger.info(f"Successfully updated request ID {request_id} status to {status}")
    else:
        logger.warning(f"No rows affected when updating request ID {request_id} status")

    return rows_affected > 0


def update_request_notified_donors(request_id: str, donor_ids: List[str]) -> bool:
    """Update the list of donors notified about a request."""
    logger.info(f"Updating notified donors for request ID {request_id} with {len(donor_ids)} donors")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Get current list
        cursor.execute('SELECT donors_notified FROM requests WHERE id = ?', (request_id,))
        result = cursor.fetchone()

        if not result:
            logger.warning(f"Request ID {request_id} not found")
            conn.close()
            return False

        current_list = []
        if result[0]:
            try:
                current_list = json.loads(result[0])
            except json.JSONDecodeError:
                logger.error(f"Error decoding donors_notified JSON for request {request_id}")

        # Add new donors to the list (avoid duplicates)
        updated_list = list(set(current_list + donor_ids))
        logger.info(f"Updating donors_notified for request {request_id}: {len(updated_list)} total donors")

        cursor.execute(
            'UPDATE requests SET donors_notified = ? WHERE id = ?',
            (json.dumps(updated_list), request_id)
        )

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            logger.info(f"Successfully updated notified donors for request ID {request_id}")
        else:
            logger.warning(f"No rows affected when updating notified donors for request ID {request_id}")

        return rows_affected > 0

    except Exception as e:
        logger.error(f"Error updating notified donors: {e}")
        conn.close()
        return False


def add_donor_to_request(request_id: str, donor_id: str) -> bool:
    """Record a donor accepting a blood request."""
    logger.info(f"Adding donor ID {donor_id} as accepted for request ID {request_id}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Get current accepted list
        cursor.execute('SELECT donors_accepted FROM requests WHERE id = ?', (request_id,))
        result = cursor.fetchone()

        if not result:
            logger.warning(f"Request ID {request_id} not found")
            conn.close()
            return False

        current_list = []
        if result[0]:
            try:
                current_list = json.loads(result[0])
            except json.JSONDecodeError:
                logger.error(f"Error decoding donors_accepted JSON for request {request_id}")

        # Add new donor to the list (avoid duplicates)
        if donor_id not in current_list:
            current_list.append(donor_id)
            logger.info(f"Adding donor {donor_id} to accepted list for request {request_id}")

        cursor.execute(
            'UPDATE requests SET donors_accepted = ? WHERE id = ?',
            (json.dumps(current_list), request_id)
        )

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            logger.info(f"Successfully added donor {donor_id} to accepted list for request {request_id}")
        else:
            logger.warning(f"No rows affected when adding donor to accepted list")

        return rows_affected > 0

    except Exception as e:
        logger.error(f"Error adding donor to request: {e}")
        conn.close()
        return False


def add_donor_to_declined_request(request_id: str, donor_id: str) -> bool:
    """Record a donor declining a blood request."""
    logger.info(f"Adding donor ID {donor_id} as declined for request ID {request_id}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Get current declined list
        cursor.execute('SELECT donors_declined FROM requests WHERE id = ?', (request_id,))
        result = cursor.fetchone()

        if not result:
            logger.warning(f"Request ID {request_id} not found")
            conn.close()
            return False

        current_list = []
        if result[0]:
            try:
                current_list = json.loads(result[0])
            except json.JSONDecodeError:
                logger.error(f"Error decoding donors_declined JSON for request {request_id}")

        # Add new donor to the list (avoid duplicates)
        if donor_id not in current_list:
            current_list.append(donor_id)
            logger.info(f"Adding donor {donor_id} to declined list for request {request_id}")

        cursor.execute(
            'UPDATE requests SET donors_declined = ? WHERE id = ?',
            (json.dumps(current_list), request_id)
        )

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            logger.info(f"Successfully added donor {donor_id} to declined list for request {request_id}")
        else:
            logger.warning(f"No rows affected when adding donor to declined list")

        return rows_affected > 0

    except Exception as e:
        logger.error(f"Error adding donor to declined request: {e}")
        conn.close()
        return False


def delete_request(request_id: str) -> bool:
    """Delete a blood request from the database."""
    logger.info(f"Deleting request ID {request_id}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('DELETE FROM requests WHERE id = ?', (request_id,))

    rows_affected = cursor.rowcount
    conn.commit()
    conn.close()

    if rows_affected > 0:
        logger.info(f"Successfully deleted request ID {request_id}")
    else:
        logger.warning(f"No request found with ID {request_id} to delete")

    return rows_affected > 0


def get_operations_stats():
    """Get statistics about donation operations."""
    logger.info("Getting donation operations statistics")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Initialize stats dictionary
    stats = {
        'total_operations': 0,
        'total_donors': 0,
        'total_requests': 0,
        'active_requests': 0,
        'fulfilled_requests': 0,
        'operations_by_blood_group': {},
        'operations_by_division': {}
    }

    try:
        # Count total donors
        cursor.execute("SELECT COUNT(*) as count FROM donors")
        stats['total_donors'] = cursor.fetchone()['count']

        # Count total requests
        cursor.execute("SELECT COUNT(*) as count FROM requests")
        stats['total_requests'] = cursor.fetchone()['count']

        # Count active requests
        cursor.execute("SELECT COUNT(*) as count FROM requests WHERE status = 'active'")
        stats['active_requests'] = cursor.fetchone()['count']

        # Count successful operations (requests with accepted donors)
        cursor.execute(
            "SELECT COUNT(*) as count FROM requests WHERE donors_accepted IS NOT NULL AND donors_accepted != '[]'")
        stats['total_operations'] = cursor.fetchone()['count']

        # Count fulfilled requests
        cursor.execute("SELECT COUNT(*) as count FROM requests WHERE status = 'fulfilled'")
        stats['fulfilled_requests'] = cursor.fetchone()['count']

        # Get operations by blood group
        cursor.execute(
            "SELECT blood_group, COUNT(*) as count FROM requests WHERE donors_accepted IS NOT NULL AND donors_accepted != '[]' GROUP BY blood_group")
        for row in cursor.fetchall():
            stats['operations_by_blood_group'][row['blood_group']] = row['count']

        # Get operations by division
        cursor.execute(
            "SELECT division, COUNT(*) as count FROM requests WHERE donors_accepted IS NOT NULL AND donors_accepted != '[]' GROUP BY division")
        for row in cursor.fetchall():
            stats['operations_by_division'][row['division']] = row['count']

    except Exception as e:
        logger.error(f"Error getting operations stats: {e}")

    conn.close()
    logger.info(
        f"Retrieved operations stats: {len(stats['operations_by_blood_group'])} blood groups, {len(stats['operations_by_division'])} divisions")
    return stats


def get_recent_operations(limit=5):
    """Get recent successful donation operations."""
    logger.info(f"Getting {limit} recent successful donation operations")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    operations = []

    try:
        # Find requests with accepted donors, ordered by most recent first
        cursor.execute("""
            SELECT * FROM requests 
            WHERE donors_accepted IS NOT NULL 
            AND donors_accepted != '[]' 
            ORDER BY request_date DESC 
            LIMIT ?
        """, (limit,))

        recent_requests = cursor.fetchall()
        logger.info(f"Found {len(recent_requests)} recent requests with accepted donors")

        for request in recent_requests:
            req_dict = dict(request)

            # Parse JSON fields
            for key in ['donors_notified', 'donors_accepted', 'donors_declined']:
                if req_dict[key]:
                    try:
                        req_dict[key] = json.loads(req_dict[key])
                    except json.JSONDecodeError:
                        logger.error(f"Error decoding {key} JSON for request {req_dict['id']}")
                        req_dict[key] = []
                else:
                    req_dict[key] = []

            # Get information about the first accepted donor
            if req_dict['donors_accepted']:
                donor_id = req_dict['donors_accepted'][0]
                cursor.execute("SELECT * FROM donors WHERE id = ?", (donor_id,))
                donor = cursor.fetchone()

                if donor:
                    donor_dict = dict(donor)
                    logger.info(f"Found donor {donor_dict['name']} for request {req_dict['id']}")

                    # Create operation entry
                    operation = {
                        'request': req_dict,
                        'donor': donor_dict,
                        'operation_date': req_dict['request_date']  # Using request date as proxy for operation date
                    }

                    operations.append(operation)
                else:
                    logger.warning(f"Donor {donor_id} referenced in request {req_dict['id']} not found")

    except Exception as e:
        logger.error(f"Error getting recent operations: {e}")

    conn.close()
    logger.info(f"Retrieved {len(operations)} recent operations")
    return operations


def get_donor_stats(donor_id):
    """Get donation statistics for a specific donor."""
    logger.info(f"Getting stats for donor ID {donor_id}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    stats = {
        'total_donations': 0,
        'pending_donations': 0,
        'fulfilled_donations': 0,
        'donor_rank': None,
        'donor_info': None
    }

    try:
        # Get donor information
        cursor.execute('SELECT * FROM donors WHERE id = ?', (donor_id,))
        donor = cursor.fetchone()

        if not donor:
            logger.warning(f"Donor not found with ID: {donor_id}")
            conn.close()
            return None  # Donor not found

        stats['donor_info'] = dict(donor)

        # Count total donations (number of times this donor has accepted requests)
        cursor.execute('''
            SELECT COUNT(*) as count FROM requests 
            WHERE donors_accepted LIKE ? AND donors_accepted IS NOT NULL
        ''', (f'%{donor_id}%',))
        result = cursor.fetchone()
        stats['total_donations'] = result['count'] if result else 0

        # Count pending donations (accepted but not fulfilled)
        cursor.execute('''
            SELECT COUNT(*) as count FROM requests 
            WHERE donors_accepted LIKE ? AND status != 'fulfilled' AND donors_accepted IS NOT NULL
        ''', (f'%{donor_id}%',))
        result = cursor.fetchone()
        stats['pending_donations'] = result['count'] if result else 0

        # Count fulfilled donations
        cursor.execute('''
            SELECT COUNT(*) as count FROM requests 
            WHERE donors_accepted LIKE ? AND status = 'fulfilled' AND donors_accepted IS NOT NULL
        ''', (f'%{donor_id}%',))
        result = cursor.fetchone()
        stats['fulfilled_donations'] = result['count'] if result else 0

        # Calculate donor rank (based on total donations)
        cursor.execute('''
            SELECT id, 
                  (SELECT COUNT(*) FROM requests WHERE donors_accepted LIKE '%' || donors.id || '%' AND donors_accepted IS NOT NULL) as donation_count
            FROM donors
            ORDER BY donation_count DESC
        ''')

        rankings = cursor.fetchall()
        for i, row in enumerate(rankings, 1):
            if str(row['id']) == str(donor_id):
                stats['donor_rank'] = i
                break

    except Exception as e:
        logger.error(f"Error getting donor stats: {e}")

    conn.close()
    logger.info(
        f"Retrieved stats for donor {donor_id}: {stats['total_donations']} donations, rank {stats['donor_rank']}")
    return stats


def get_top_donors(limit=3, time_period=None):
    """Get top donors based on number of donations.

    Args:
        limit (int): Number of top donors to retrieve
        time_period (str, optional): 'month' for this month, 'year' for this year, None for all time
    """
    if time_period:
        logger.info(f"Getting top {limit} donors for time period: {time_period}")
    else:
        logger.info(f"Getting top {limit} donors of all time")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    top_donors = []

    try:
        time_filter = ""
        if time_period == 'month':
            # Filter for current month
            current_month = datetime.now().strftime('%Y-%m')
            time_filter = f"AND request_date LIKE '{current_month}%'"
        elif time_period == 'year':
            # Filter for current year
            current_year = datetime.now().strftime('%Y')
            time_filter = f"AND request_date LIKE '{current_year}%'"

        # This query finds donors with the most donations
        query = f'''
            SELECT 
                donors.id,
                donors.name, 
                donors.blood_group,
                COUNT(DISTINCT requests.id) as donation_count
            FROM 
                donors
            LEFT JOIN 
                requests ON requests.donors_accepted LIKE '%' || donors.id || '%' AND requests.donors_accepted IS NOT NULL {time_filter}
            GROUP BY 
                donors.id
            ORDER BY 
                donation_count DESC
            LIMIT ?
        '''

        cursor.execute(query, (limit,))
        results = cursor.fetchall()

        for row in results:
            if row['donation_count'] > 0:  # Only include donors who have made donations
                top_donors.append({
                    'id': row['id'],
                    'name': row['name'],
                    'blood_group': row['blood_group'],
                    'donation_count': row['donation_count']
                })

    except Exception as e:
        logger.error(f"Error getting top donors: {e}")

    conn.close()
    logger.info(f"Retrieved {len(top_donors)} top donors")
    return top_donors


def get_requests_by_location(division: str, district: str = None) -> List[Dict[str, Any]]:
    """Get blood requests from a specific location.

    Args:
        division (str): Division name
        district (str, optional): District name. If None, return all requests in division.

    Returns:
        List of request dictionaries
    """
    if district:
        logger.info(f"Getting active requests by location - Division: {division}, District: {district}")
    else:
        logger.info(f"Getting active requests by location - Division: {division}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        if district:
            # Get requests from specific district and division
            cursor.execute(
                '''SELECT * FROM requests 
                WHERE LOWER(division) = LOWER(?) 
                AND LOWER(district) = LOWER(?) 
                AND status = 'active'
                ORDER BY request_date DESC''',
                (division.lower(), district.lower())
            )
        else:
            # Get all requests from division
            cursor.execute(
                '''SELECT * FROM requests 
                WHERE LOWER(division) = LOWER(?) 
                AND status = 'active'
                ORDER BY request_date DESC''',
                (division.lower(),)
            )

        results = cursor.fetchall()

        result_list = []
        for row in results:
            row_dict = dict(row)
            # Parse JSON string lists to Python lists
            for key in ['donors_notified', 'donors_accepted', 'donors_declined']:
                if row_dict[key]:
                    try:
                        row_dict[key] = json.loads(row_dict[key])
                    except json.JSONDecodeError:
                        logger.error(f"Error decoding {key} JSON for request {row_dict['id']}")
                        row_dict[key] = []
                else:
                    row_dict[key] = []
            result_list.append(row_dict)

        conn.close()
        logger.info(f"Found {len(result_list)} active requests in specified location")
        return result_list

    except Exception as e:
        logger.error(f"Error getting requests by location: {e}")
        conn.close()
        return []


def store_chat_group_info(request_id: str, donor_id: str, chat_id: int, invite_link: str) -> None:
    """
    Store chat group information in the database.

    Args:
        request_id: ID of the blood request
        donor_id: ID of the accepting donor
        chat_id: ID of the created chat group
        invite_link: Invite link to the chat group
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Insert the chat group information
        cursor.execute('''
        INSERT INTO chat_groups (request_id, donor_id, chat_id, invite_link, created_at)
        VALUES (?, ?, ?, ?, ?)
        ''', (
            request_id,
            donor_id,
            chat_id,
            invite_link,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))

        conn.commit()
        conn.close()
        logger.info(f"Successfully stored chat group info for request {request_id} and donor {donor_id}")

    except Exception as e:
        logger.error(f"Error storing chat group information: {e}")


def get_chat_group_by_request(request_id: str):
    """
    Get chat group information for a specific request.

    Args:
        request_id: ID of the blood request

    Returns:
        Dictionary with chat group information or None if not found
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
        SELECT * FROM chat_groups WHERE request_id = ? ORDER BY created_at DESC LIMIT 1
        ''', (request_id,))

        result = cursor.fetchone()
        conn.close()

        if result:
            return dict(result)
        return None

    except Exception as e:
        logger.error(f"Error getting chat group information: {e}")
        return None


# Clear the database (if needed) - comment this out by default
def clear_database():
    """Clear all data from the database for a fresh start."""
    logger.warning("CLEARING ALL DATABASE TABLES!")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Drop existing tables if they exist
        cursor.execute("DROP TABLE IF EXISTS donors")
        cursor.execute("DROP TABLE IF EXISTS requests")
        cursor.execute("DROP TABLE IF EXISTS chat_groups")

        conn.commit()
        logger.warning("All tables dropped from database")

        # Recreate tables
        create_tables()
        logger.info("Tables recreated successfully")
    except Exception as e:
        logger.error(f"Error clearing database: {e}")
    finally:
        conn.close()


# Uncomment the following line to clear the database (use with caution!)
#clear_database()


# Check database structure and contents (for debugging)
def check_database():
    """Check database structure and contents for debugging."""
    logger.info("Checking database...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [table[0] for table in cursor.fetchall()]
        logger.info(f"Tables in database: {tables}")

        # Check donors
        if 'donors' in tables:
            cursor.execute("SELECT COUNT(*) as count FROM donors")
            donor_count = cursor.fetchone()['count']
            logger.info(f"Donors count: {donor_count}")

            if donor_count > 0:
                cursor.execute("SELECT * FROM donors LIMIT 1")
                sample_donor = dict(cursor.fetchone())
                logger.info(f"Sample donor: {sample_donor}")

        # Check requests
        if 'requests' in tables:
            cursor.execute("SELECT COUNT(*) as count FROM requests")
            request_count = cursor.fetchone()['count']
            logger.info(f"Requests count: {request_count}")

            if request_count > 0:
                cursor.execute("SELECT * FROM requests LIMIT 1")
                sample_request = dict(cursor.fetchone())
                logger.info(f"Sample request: {sample_request}")

    except Exception as e:
        logger.error(f"Error checking database: {e}")
    finally:
        conn.close()


def update_request_field(request_id: str, field: str, value: Any) -> bool:
    """Update a specific field of a request."""
    logger.info(f"Updating request ID {request_id} field {field} to: {value}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute(
            f'UPDATE requests SET {field} = ? WHERE id = ?',
            (value, request_id)
        )

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            logger.info(f"Successfully updated request ID {request_id} field {field}")
        else:
            logger.warning(f"No rows affected when updating request ID {request_id} field {field}")

        return rows_affected > 0
    except Exception as e:
        logger.error(f"Error updating request field: {e}")
        conn.close()
        return False


def update_donor_restriction(donor_id: str, is_restricted: bool) -> bool:
    """Update a donor's restriction status."""
    logger.info(f"Updating donor ID {donor_id} restriction status to: {is_restricted}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Check if is_restricted column exists, create if not
        cursor.execute("PRAGMA table_info(donors)")
        columns = [column[1] for column in cursor.fetchall()]

        if 'is_restricted' not in columns:
            cursor.execute('ALTER TABLE donors ADD COLUMN is_restricted INTEGER DEFAULT 0')
            logger.info("Added is_restricted column to donors table")

        # Update the restriction status
        cursor.execute(
            'UPDATE donors SET is_restricted = ? WHERE id = ?',
            (1 if is_restricted else 0, donor_id)
        )

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            logger.info(f"Successfully updated donor ID {donor_id} restriction status")
        else:
            logger.warning(f"No rows affected when updating donor ID {donor_id} restriction status")

        return rows_affected > 0
    except Exception as e:
        logger.error(f"Error updating donor restriction: {e}")
        conn.close()
        return False


def get_donation_history(donor_id: str) -> List[Dict[str, Any]]:
    """Get detailed donation history for a specific donor."""
    logger.info(f"Getting donation history for donor ID: {donor_id}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    donation_history = []

    try:
        # Find all requests where this donor was accepted
        cursor.execute("""
            SELECT * FROM requests 
            WHERE donors_accepted LIKE ? 
            ORDER BY request_date DESC
        """, (f'%{donor_id}%',))

        requests = cursor.fetchall()
        logger.info(f"Found {len(requests)} donation records for donor {donor_id}")

        for req in requests:
            req_dict = dict(req)

            # Parse JSON fields
            for key in ['donors_notified', 'donors_accepted', 'donors_declined']:
                if req_dict[key]:
                    try:
                        req_dict[key] = json.loads(req_dict[key])
                    except json.JSONDecodeError:
                        logger.error(f"Error decoding {key} JSON for request {req_dict['id']}")
                        req_dict[key] = []
                else:
                    req_dict[key] = []

            # Add to history
            donation_history.append({
                'request_id': req_dict['id'],
                'patient_name': req_dict['name'],
                'blood_group': req_dict['blood_group'],
                'hospital': req_dict['hospital_name'],
                'date': req_dict['request_date'],
                'status': req_dict['status']
            })

    except Exception as e:
        logger.error(f"Error getting donation history: {e}")

    conn.close()
    return donation_history
def search_donors(search_term: str) -> List[Dict[str, Any]]:
    """Search for donors using a search term."""
    logger.info(f"Searching for donors with term: {search_term}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # Use LIKE for case-insensitive partial matching on multiple fields
        cursor.execute("""
            SELECT * FROM donors
            WHERE name LIKE ? 
            OR blood_group LIKE ? 
            OR division LIKE ? 
            OR district LIKE ?
            OR area LIKE ? 
            OR phone LIKE ?
            ORDER BY name
        """, (
            f'%{search_term}%',
            f'%{search_term}%',
            f'%{search_term}%',
            f'%{search_term}%',
            f'%{search_term}%',
            f'%{search_term}%'
        ))

        results = cursor.fetchall()
        conn.close()

        donors = [dict(row) for row in results]
        logger.info(f"Found {len(donors)} donors matching '{search_term}'")
        return donors

    except Exception as e:
        logger.error(f"Error searching for donors: {e}")
        conn.close()
        return []


def store_support_message(user_info, message):
    """Store support message in the database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            user_name TEXT,
            message TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT NOT NULL
        )
        ''')

        # Get user name
        first_name = user_info.get('first_name', '')
        last_name = user_info.get('last_name', '')
        user_name = f"{first_name} {last_name}".strip()

        # Insert message
        cursor.execute('''
        INSERT INTO support_messages (user_id, user_name, message, created_at)
        VALUES (?, ?, ?, ?)
        ''', (
            user_info.get('id'),
            user_name,
            message,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))

        conn.commit()
        conn.close()
        logger.info(f"Support message from {user_name} stored successfully")
        return True
    except Exception as e:
        logger.error(f"Error storing support message: {e}")
        return False


def get_support_messages():
    """Get all support messages from the database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            user_name TEXT,
            message TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT NOT NULL
        )
        ''')

        # Get messages ordered by most recent first
        cursor.execute('''
        SELECT * FROM support_messages
        ORDER BY created_at DESC
        ''')

        messages = cursor.fetchall()

        # Convert to list of dicts
        result = []
        for msg in messages:
            result.append(dict(msg))

        conn.close()
        return result
    except Exception as e:
        logger.error(f"Error getting support messages: {e}")
        return []


def record_admin_reply(user_id, reply_message):
    """Record admin reply to user's support message."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS admin_replies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            reply_message TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        ''')

        # Insert reply
        cursor.execute('''
        INSERT INTO admin_replies (user_id, reply_message, created_at)
        VALUES (?, ?, ?)
        ''', (
            user_id,
            reply_message,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))

        # Update support messages status to 'replied' for this user
        cursor.execute('''
        UPDATE support_messages
        SET status = 'replied'
        WHERE user_id = ? AND status = 'pending'
        ''', (user_id,))

        conn.commit()
        conn.close()
        logger.info(f"Admin reply to user {user_id} recorded successfully")
        return True
    except Exception as e:
        logger.error(f"Error recording admin reply: {e}")
        return False


def initialize_database():
    """Initialize all database tables if they don't exist."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create support_messages table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            user_name TEXT,
            message TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT NOT NULL
        )
        ''')

        # Create admin_replies table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS admin_replies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            reply_message TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        ''')

        # Add any other tables here...

        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return False


def save_broadcast_message(admin_id, message_text, target_type='all'):
    """Save a broadcast message to the database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Check if broadcast_messages table exists, create if not
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS broadcast_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            message_text TEXT,
            target_type TEXT,
            sent_date TEXT,
            recipient_count INTEGER DEFAULT 0
        )
        ''')

        # Insert the broadcast message
        cursor.execute('''
        INSERT INTO broadcast_messages 
        (admin_id, message_text, target_type, sent_date)
        VALUES (?, ?, ?, ?)
        ''', (admin_id, message_text, target_type, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        broadcast_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return broadcast_id
    except Exception as e:
        logger.error(f"Error saving broadcast message: {e}")
        return None


def update_broadcast_recipient_count(broadcast_id, count):
    """Update the recipient count for a broadcast message."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute('''
        UPDATE broadcast_messages
        SET recipient_count = ?
        WHERE id = ?
        ''', (count, broadcast_id))

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error updating broadcast recipient count: {e}")
        return False


def get_recent_broadcasts(limit=10):
    """Get recent broadcast messages."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
        SELECT * FROM broadcast_messages
        ORDER BY sent_date DESC
        LIMIT ?
        ''', (limit,))

        broadcasts = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return broadcasts
    except Exception as e:
        logger.error(f"Error getting recent broadcasts: {e}")
        return []


def save_personalized_message(admin_id, user_id, message_text):
    """Save a personalized message to the database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Check if personalized_messages table exists, create if not
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS personalized_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            user_id INTEGER,
            message_text TEXT,
            sent_date TEXT,
            status TEXT DEFAULT 'sent'
        )
        ''')

        # Insert the personalized message
        cursor.execute('''
        INSERT INTO personalized_messages 
        (admin_id, user_id, message_text, sent_date)
        VALUES (?, ?, ?, ?)
        ''', (admin_id, user_id, message_text, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        message_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return message_id
    except Exception as e:
        logger.error(f"Error saving personalized message: {e}")
        return None


def get_user_messages(user_id, limit=10):
    """Get messages sent to a specific user."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
        SELECT * FROM personalized_messages
        WHERE user_id = ?
        ORDER BY sent_date DESC
        LIMIT ?
        ''', (user_id, limit))

        messages = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return messages
    except Exception as e:
        logger.error(f"Error getting user messages: {e}")
        return []




# Run a check when module is imported
check_database()