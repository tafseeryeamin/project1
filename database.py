import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('database')

# Database connection
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql:///blood_bot')

# Handle Railway's PostgreSQL URL format
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

logger.info(f"Database URL format: {DATABASE_URL[:12]}...") # Log format without exposing credentials

def get_db_connection():
    """Get a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def initialize_database():
    """Initialize database tables if they don't exist."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create donors table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS donors (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            name VARCHAR(100) NOT NULL,
            age VARCHAR(20),
            phone VARCHAR(20),
            district VARCHAR(50),
            division VARCHAR(50),
            area VARCHAR(100),
            blood_group VARCHAR(5),
            gender VARCHAR(10),
            registration_date TIMESTAMP,
            is_restricted BOOLEAN DEFAULT FALSE
        )
        ''')
        
        # Create requests table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS requests (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT NOT NULL,
            name VARCHAR(100) NOT NULL,
            age VARCHAR(20),
            hospital_name VARCHAR(100),
            hospital_address TEXT,
            area VARCHAR(100),
            division VARCHAR(50),
            district VARCHAR(50),
            urgency VARCHAR(20),
            phone VARCHAR(20),
            blood_group VARCHAR(5),
            request_date TIMESTAMP,
            status VARCHAR(20) DEFAULT 'active',
            notified_donors TEXT
        )
        ''')
        
        # Create donations table to track accepted donations
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS donations (
            id SERIAL PRIMARY KEY,
            request_id INTEGER REFERENCES requests(id),
            donor_id INTEGER REFERENCES donors(id),
            status VARCHAR(20) DEFAULT 'pending',
            acceptance_date TIMESTAMP,
            completion_date TIMESTAMP,
            notes TEXT
        )
        ''')
        
        # Create support messages table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS support_messages (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            user_name VARCHAR(100),
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) DEFAULT 'pending'
        )
        ''')
        
        # Create admin replies table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS admin_replies (
            id SERIAL PRIMARY KEY,
            admin_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            message TEXT,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Create broadcast messages table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS broadcast_messages (
            id SERIAL PRIMARY KEY,
            admin_id BIGINT NOT NULL,
            message_text TEXT,
            target_type VARCHAR(20),
            sent_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            recipient_count INTEGER DEFAULT 0
        )
        ''')
        
        # Create personalized messages table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS personalized_messages (
            id SERIAL PRIMARY KEY,
            admin_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            message_text TEXT,
            sent_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Database initialized successfully")
        return True
    except Exception as e:
        print(f"Error initializing database: {e}")
        return False

# Donor functions
def save_donor(donor_data):
    """Save a new donor to the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO donors (
            telegram_id, name, age, phone, district, division, area, blood_group, gender, registration_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        ''', (
            donor_data['telegram_id'],
            donor_data['name'],
            donor_data['age'],
            donor_data['phone'],
            donor_data['district'],
            donor_data['division'],
            donor_data['area'],
            donor_data['blood_group'],
            donor_data['gender'],
            donor_data['registration_date']
        ))
        
        donor_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        
        return donor_id
    except Exception as e:
        print(f"Error saving donor: {e}")
        return None

def get_donor_by_telegram_id(telegram_id):
    """Get donor information by Telegram ID."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('SELECT * FROM donors WHERE telegram_id = %s', (telegram_id,))
        donor = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return donor
    except Exception as e:
        print(f"Error getting donor: {e}")
        return None

def get_donor_by_id(donor_id):
    """Get donor information by ID."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('SELECT * FROM donors WHERE id = %s', (donor_id,))
        donor = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return donor
    except Exception as e:
        print(f"Error getting donor: {e}")
        return None

def update_donor(donor_id, update_data):
    """Update donor information."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build the SQL query dynamically based on the fields to update
        sql_parts = []
        values = []
        
        for key, value in update_data.items():
            sql_parts.append(f"{key} = %s")
            values.append(value)
        
        # Add the donor_id as the last value
        values.append(donor_id)
        
        sql = f"UPDATE donors SET {', '.join(sql_parts)} WHERE id = %s"
        
        cursor.execute(sql, values)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error updating donor: {e}")
        return False

def get_all_donors():
    """Get all registered donors."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('SELECT * FROM donors ORDER BY registration_date DESC')
        donors = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return donors
    except Exception as e:
        print(f"Error getting all donors: {e}")
        return []

def search_donors(search_term):
    """Search for donors by name, blood group, or location."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Create a search pattern for LIKE queries
        search_pattern = f"%{search_term}%"
        
        cursor.execute('''
        SELECT * FROM donors 
        WHERE 
            lower(name) LIKE lower(%s) OR 
            lower(blood_group) LIKE lower(%s) OR 
            lower(district) LIKE lower(%s) OR 
            lower(division) LIKE lower(%s) OR
            lower(phone) LIKE lower(%s)
        ORDER BY registration_date DESC
        ''', (search_pattern, search_pattern, search_pattern, search_pattern, search_pattern))
        
        donors = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return donors
    except Exception as e:
        print(f"Error searching donors: {e}")
        return []

def get_donors_by_blood_groups(blood_groups):
    """Get donors with specific blood groups."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        placeholders = ', '.join(['%s'] * len(blood_groups))
        query = f'SELECT * FROM donors WHERE blood_group IN ({placeholders})'
        
        cursor.execute(query, blood_groups)
        donors = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return donors
    except Exception as e:
        print(f"Error getting donors by blood groups: {e}")
        return []

def delete_donor(donor_id):
    """Delete a donor from the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM donors WHERE id = %s', (donor_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error deleting donor: {e}")
        return False

def update_donor_restriction(donor_id, is_restricted):
    """Update donor restriction status."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('UPDATE donors SET is_restricted = %s WHERE id = %s', (is_restricted, donor_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error updating donor restriction: {e}")
        return False

def get_donor_stats(donor_id):
    """Get statistics for a specific donor."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get total donations
        cursor.execute('''
        SELECT COUNT(*) as total_donations FROM donations 
        WHERE donor_id = %s
        ''', (donor_id,))
        total_donations = cursor.fetchone()['total_donations']
        
        # Get fulfilled donations
        cursor.execute('''
        SELECT COUNT(*) as fulfilled_donations FROM donations 
        WHERE donor_id = %s AND status = 'completed'
        ''', (donor_id,))
        fulfilled_donations = cursor.fetchone()['fulfilled_donations']
        
        # Get pending donations
        cursor.execute('''
        SELECT COUNT(*) as pending_donations FROM donations 
        WHERE donor_id = %s AND status = 'pending'
        ''', (donor_id,))
        pending_donations = cursor.fetchone()['pending_donations']
        
        # Get donor rank
        cursor.execute('''
        WITH donor_ranks AS (
            SELECT 
                d.id, 
                COUNT(don.id) as donation_count,
                RANK() OVER (ORDER BY COUNT(don.id) DESC) as donor_rank
            FROM 
                donors d
            LEFT JOIN 
                donations don ON d.id = don.donor_id
            GROUP BY 
                d.id
        )
        SELECT donor_rank FROM donor_ranks WHERE id = %s
        ''', (donor_id,))
        
        rank_result = cursor.fetchone()
        donor_rank = rank_result['donor_rank'] if rank_result else None
        
        cursor.close()
        conn.close()
        
        return {
            'total_donations': total_donations,
            'fulfilled_donations': fulfilled_donations,
            'pending_donations': pending_donations,
            'donor_rank': donor_rank
        }
    except Exception as e:
        print(f"Error getting donor stats: {e}")
        return {
            'total_donations': 0,
            'fulfilled_donations': 0,
            'pending_donations': 0,
            'donor_rank': None
        }

def get_top_donors(limit=10, period=None):
    """Get top donors by donation count."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        time_condition = ""
        if period == 'month':
            time_condition = "AND EXTRACT(MONTH FROM don.acceptance_date) = EXTRACT(MONTH FROM CURRENT_DATE) AND EXTRACT(YEAR FROM don.acceptance_date) = EXTRACT(YEAR FROM CURRENT_DATE)"
        elif period == 'year':
            time_condition = "AND EXTRACT(YEAR FROM don.acceptance_date) = EXTRACT(YEAR FROM CURRENT_DATE)"
        
        cursor.execute(f'''
        SELECT 
            d.id, d.name, d.blood_group,
            COUNT(don.id) as donation_count
        FROM 
            donors d
        JOIN 
            donations don ON d.id = don.donor_id
        WHERE 
            don.status = 'completed'
            {time_condition}
        GROUP BY 
            d.id, d.name, d.blood_group
        ORDER BY 
            donation_count DESC
        LIMIT %s
        ''', (limit,))
        
        top_donors = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return top_donors
    except Exception as e:
        print(f"Error getting top donors: {e}")
        return []

# Request functions
def save_request(request_data):
    """Save a new blood request to the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO requests (
            telegram_id, name, age, hospital_name, hospital_address, 
            area, division, district, urgency, phone, blood_group, request_date, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        ''', (
            request_data['telegram_id'],
            request_data['name'],
            request_data['age'],
            request_data['hospital_name'],
            request_data['hospital_address'],
            request_data['area'],
            request_data['division'],
            request_data['district'],
            request_data['urgency'],
            request_data['phone'],
            request_data['blood_group'],
            request_data['request_date'],
            request_data['status']
        ))
        
        request_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        
        return request_id
    except Exception as e:
        print(f"Error saving request: {e}")
        return None

def get_request_by_id(request_id):
    """Get request information by ID."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('SELECT * FROM requests WHERE id = %s', (request_id,))
        request = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return request
    except Exception as e:
        print(f"Error getting request: {e}")
        return None

def get_active_requests():
    """Get all active blood requests."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('''
        SELECT * FROM requests 
        WHERE status = 'active' 
        ORDER BY request_date DESC
        ''')
        
        requests = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return requests
    except Exception as e:
        print(f"Error getting active requests: {e}")
        return []

def get_requests_by_location(division, district=None):
    """Get active requests by location."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        if district:
            cursor.execute('''
            SELECT * FROM requests 
            WHERE status = 'active' 
            AND lower(division) = lower(%s) 
            AND lower(district) = lower(%s)
            ORDER BY request_date DESC
            ''', (division, district))
        else:
            cursor.execute('''
            SELECT * FROM requests 
            WHERE status = 'active' 
            AND lower(division) = lower(%s)
            ORDER BY request_date DESC
            ''', (division,))
        
        requests = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return requests
    except Exception as e:
        print(f"Error getting requests by location: {e}")
        return []

def update_request_status(request_id, status):
    """Update the status of a request."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        UPDATE requests 
        SET status = %s 
        WHERE id = %s
        ''', (status, request_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error updating request status: {e}")
        return False

def update_request_field(request_id, field, value):
    """Update a specific field in a request."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        sql = f"UPDATE requests SET {field} = %s WHERE id = %s"
        cursor.execute(sql, (value, request_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error updating request field: {e}")
        return False

def update_request_notified_donors(request_id, donor_ids):
    """Update the list of notified donors for a request."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Convert list of IDs to comma-separated string
        donor_ids_str = ','.join(str(id) for id in donor_ids)
        
        cursor.execute('''
        UPDATE requests 
        SET notified_donors = %s 
        WHERE id = %s
        ''', (donor_ids_str, request_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error updating notified donors: {e}")
        return False

def delete_request(request_id):
    """Delete a request from the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First delete related donations
        cursor.execute('DELETE FROM donations WHERE request_id = %s', (request_id,))
        
        # Then delete the request
        cursor.execute('DELETE FROM requests WHERE id = %s', (request_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error deleting request: {e}")
        return False

# Donation functions
def add_donor_to_request(request_id, donor_id):
    """Add a donor to a request (donor accepts a blood request)."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if this donation already exists
        cursor.execute('''
        SELECT id FROM donations
        WHERE request_id = %s AND donor_id = %s
        ''', (request_id, donor_id))
        
        existing = cursor.fetchone()
        
        if existing:
            # Update existing donation
            cursor.execute('''
            UPDATE donations
            SET status = 'pending', acceptance_date = %s
            WHERE request_id = %s AND donor_id = %s
            ''', (datetime.now(), request_id, donor_id))
        else:
            # Create new donation
            cursor.execute('''
            INSERT INTO donations (request_id, donor_id, status, acceptance_date)
            VALUES (%s, %s, 'pending', %s)
            ''', (request_id, donor_id, datetime.now()))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error adding donor to request: {e}")
        return False

def add_donor_to_declined_request(request_id, donor_id):
    """Record that a donor declined a request."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if this donation already exists
        cursor.execute('''
        SELECT id FROM donations
        WHERE request_id = %s AND donor_id = %s
        ''', (request_id, donor_id))
        
        existing = cursor.fetchone()
        
        if existing:
            # Update existing donation
            cursor.execute('''
            UPDATE donations
            SET status = 'declined', acceptance_date = %s
            WHERE request_id = %s AND donor_id = %s
            ''', (datetime.now(), request_id, donor_id))
        else:
            # Create new donation record with declined status
            cursor.execute('''
            INSERT INTO donations (request_id, donor_id, status, acceptance_date)
            VALUES (%s, %s, 'declined', %s)
            ''', (request_id, donor_id, datetime.now()))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error recording declined request: {e}")
        return False

def get_recent_operations(limit=10):
    """Get recent successful donation operations."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('''
        SELECT 
            d.id as donation_id,
            d.acceptance_date as operation_date,
            r.* as request,
            dnr.* as donor
        FROM 
            donations d
        JOIN 
            requests r ON d.request_id = r.id
        JOIN 
            donors dnr ON d.donor_id = dnr.id
        WHERE 
            d.status = 'pending' OR d.status = 'completed'
        ORDER BY 
            d.acceptance_date DESC
        LIMIT %s
        ''', (limit,))
        
        # This will return a list of dictionaries with nested 'request' and 'donor' dictionaries
        operations = []
        rows = cursor.fetchall()
        
        for row in rows:
            # Extract and reshape the data
            operation = {
                'id': row['donation_id'],
                'operation_date': row['operation_date'],
                'request': {},
                'donor': {}
            }
            
            # Populate request data
            for key in row.keys():
                if key.startswith('request_'):
                    clean_key = key[8:]  # Remove 'request_' prefix
                    operation['request'][clean_key] = row[key]
            
            # Populate donor data
            for key in row.keys():
                if key.startswith('donor_'):
                    clean_key = key[6:]  # Remove 'donor_' prefix
                    operation['donor'][clean_key] = row[key]
            
            operations.append(operation)
        
        cursor.close()
        conn.close()
        
        return operations
    except Exception as e:
        print(f"Error getting recent operations: {e}")
        return []

def get_operations_stats():
    """Get donation operation statistics."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get total donors
        cursor.execute('SELECT COUNT(*) as total_donors FROM donors')
        total_donors = cursor.fetchone()['total_donors']
        
        # Get total requests
        cursor.execute('SELECT COUNT(*) as total_requests FROM requests')
        total_requests = cursor.fetchone()['total_requests']
        
        # Get active requests
        cursor.execute('SELECT COUNT(*) as active_requests FROM requests WHERE status = %s', ('active',))
        active_requests = cursor.fetchone()['active_requests']
        
        # Get total operations (successful donations)
        cursor.execute('''
        SELECT COUNT(*) as total_operations 
        FROM donations 
        WHERE status = 'pending' OR status = 'completed'
        ''')
        total_operations = cursor.fetchone()['total_operations']
        
        cursor.close()
        conn.close()
        
        return {
            'total_donors': total_donors,
            'total_requests': total_requests,
            'active_requests': active_requests,
            'total_operations': total_operations
        }
    except Exception as e:
        print(f"Error getting operations stats: {e}")
        return {
            'total_donors': 0,
            'total_requests': 0,
            'active_requests': 0,
            'total_operations': 0
        }

# Support message functions
def store_support_message(user_info, message):
    """Store a support message from a user."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        user_id = user_info.get('id')
        user_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip()
        
        cursor.execute('''
        INSERT INTO support_messages (user_id, user_name, message, created_at, status)
        VALUES (%s, %s, %s, %s, %s)
        ''', (user_id, user_name, message, datetime.now(), 'pending'))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error storing support message: {e}")
        return False

def get_support_messages():
    """Get all support messages."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('''
        SELECT * FROM support_messages
        ORDER BY created_at DESC
        ''')
        
        messages = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return messages
    except Exception as e:
        print(f"Error getting support messages: {e}")
        return []

def record_admin_reply(user_id, message):
    """Record an admin reply to a user."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        admin_id = os.getenv('ADMIN_ID', '0')
        
        cursor.execute('''
        INSERT INTO admin_replies (admin_id, user_id, message, sent_at)
        VALUES (%s, %s, %s, %s)
        ''', (admin_id, user_id, message, datetime.now()))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error recording admin reply: {e}")
        return False

# Broadcast message functions
def save_broadcast_message(admin_id, message_text, target_type='all'):
    """Save a broadcast message sent by an admin."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO broadcast_messages (admin_id, message_text, target_type, sent_date)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        ''', (admin_id, message_text, target_type, datetime.now()))
        
        broadcast_id = cursor.fetchone()[0]
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return broadcast_id
    except Exception as e:
        print(f"Error saving broadcast message: {e}")
        return None

def update_broadcast_recipient_count(broadcast_id, count):
    """Update the recipient count for a broadcast message."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        UPDATE broadcast_messages
        SET recipient_count = %s
        WHERE id = %s
        ''', (count, broadcast_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        print(f"Error updating broadcast recipient count: {e}")
        return False

def get_recent_broadcasts(limit=10):
    """Get recent broadcast messages."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('''
        SELECT * FROM broadcast_messages
        ORDER BY sent_date DESC
        LIMIT %s
        ''', (limit,))
        
        broadcasts = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return broadcasts
    except Exception as e:
        print(f"Error getting recent broadcasts: {e}")
        return []

def save_personalized_message(admin_id, user_id, message_text):
    """Save a personalized message sent by an admin to a specific user."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO personalized_messages (admin_id, user_id, message_text, sent_date)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        ''', (admin_id, user_id, message_text, datetime.now()))
        
        message_id = cursor.fetchone()[0]
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return message_id
    except Exception as e:
        print(f"Error saving personalized message: {e}")
        return None
