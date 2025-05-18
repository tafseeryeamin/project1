import logging
import asyncio
import os
from datetime import datetime
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler, \
    CallbackQueryHandler
from telegram import Update
from telegram.ext import ContextTypes
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from locations import BANGLADESH_DIVISIONS, BANGLADESH_DISTRICTS, ALL_DISTRICTS, get_division_for_district
import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
import database as db
from database import initialize_database
load_dotenv()
logger = logging.getLogger(__name__)
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_ID = os.getenv('ADMIN_ID')
# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql:///blood_bot')

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


logger = logging.getLogger(__name__)

# Define conversation states for various flows
# Original states
DONOR_NAME, DONOR_AGE, DONOR_PHONE, DONOR_DISTRICT, DONOR_DIVISION, DONOR_AREA, DONOR_BLOOD_GROUP, DONOR_GENDER = range(
    8)
REQUEST_NAME, REQUEST_AGE, REQUEST_HOSPITAL_NAME, REQUEST_HOSPITAL_ADDRESS, REQUEST_AREA, REQUEST_DIVISION, REQUEST_DISTRICT, REQUEST_URGENCY, REQUEST_PHONE, REQUEST_BLOOD_GROUP = range(
    10)
DONOR_TERMS_AGREEMENT = 999
SUPPORT_MESSAGE = 900
CONFIRM_SEND_SUPPORT = 901
ADMIN_REPLY_MESSAGE = 902
# New states for direct registration and post-acceptance
DIRECT_DONOR_BLOOD_GROUP, DIRECT_DONOR_DIVISION, DIRECT_DONOR_DISTRICT = range(300, 303)
DONOR_TERMS_AFTER_ACCEPT, DONOR_NAME_AFTER_ACCEPT, DONOR_PHONE_AFTER_ACCEPT = range(400, 403)

# Create the terms and conditions text for initial registration
DONOR_TERMS_TEXT = """
🩸 *BLOOD DONATION TERMS AND CONDITIONS*

Before registering as a blood donor, please read and agree to the following terms:

1️⃣ *Personal Information*
   • Your contact information (phone number) will be shared with blood recipients when you accept a donation request
   • Your location will be used to match you with nearby donation requests
   • Your basic profile (name, blood type, area) may be visible to other users

2️⃣ *Communication*
   • You will receive notifications when your blood type is needed
   • You may be contacted directly by patients or their representatives
   • The bot administrators may contact you regarding donations

3️⃣ *Health & Safety*
   • You confirm that you are in good health and eligible to donate blood
   • You will inform recipients of any health conditions that might affect donation
   • You will follow all safety protocols at donation facilities

4️⃣ *Commitment*
   • While accepting a request is voluntary, we encourage you to fulfill your commitment once accepted
   • If you can no longer donate after accepting, please notify the patient promptly
   • You agree to keep your donor profile updated with current information

5️⃣ *Privacy & Data Security*
   • Your information is stored securely and only used for donation purposes
   • Your donation history will be tracked for statistical purposes
   • You may request removal of your information at any time

By pressing "I Agree" below, you consent to these terms and conditions.
"""

# Create the agreement text for when donors accept a request
DONATION_TERMS_TEXT = """
🩸 *DONATION CONSENT AND DATA SHARING AGREEMENT*

Before proceeding with your blood donation, please read and agree to the following terms:

1️⃣ *Personal Information*
   • Your name and phone number will be shared with the blood recipient
   • This information will only be used for this specific donation
   • The recipient may contact you directly to coordinate the donation

2️⃣ *Commitment*
   • By accepting this request, you are committing to donate blood to the patient
   • If you cannot fulfill this commitment, please notify the recipient promptly
   • You understand the importance of your donation for the patient's health

3️⃣ *Medical Responsibility*
   • You confirm that you are eligible to donate blood
   • You will inform the hospital of any health conditions that might affect donation
   • You will follow all safety protocols at the hospital or donation facility

4️⃣ *Privacy*
   • Your contact information will be used only for this donation
   • You may be contacted for follow-up if needed

By pressing "I Agree" below, you consent to these terms and conditions.
"""


# Start command handler - Immediately begins donor registration if user is new
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start command handler that immediately begins donor registration if user is new."""
    user = update.effective_user

    # Check if user is already registered as a donor
    donor = db.get_donor_by_telegram_id(user.id)

    if donor:
        # User is already a donor, show donor dashboard and options
        keyboard = [
            [InlineKeyboardButton("📊 My Donor Dashboard", callback_data='open_donor_dashboard')],
            [InlineKeyboardButton("Request Blood", callback_data='request_blood')],
            [InlineKeyboardButton("View Donors", callback_data='view_donors')],
            [InlineKeyboardButton("📬 Contact Support", callback_data='open_support')]
        ]

        # Add admin button if the user is an admin
        if update.effective_user.id == int(os.getenv('ADMIN_ID', '0')):
            keyboard.append([InlineKeyboardButton("👑 Admin Dashboard", callback_data='open_admin_dashboard')])

        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            f'Welcome back {user.first_name}! You are already registered as a blood donor.\n\n'
            f'Your blood group: {donor["blood_group"]}\n'
            f'Your location: {donor["district"]}, {donor["division"]}\n\n'
            'What would you like to do?',
            reply_markup=reply_markup
        )

        # Also show recent blood requests for the donor
        await show_recent_matching_requests(update, context, donor)
        return ConversationHandler.END
    else:
        # User is not registered, immediately start donor registration with blood group
        keyboard = [
            ['A+', 'A-', 'B+', 'B-'],
            ['AB+', 'AB-', 'O+', 'O-']
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)

        await update.message.reply_text(
            f'Hello {user.first_name}! Welcome to the Blood Donation Bot.\n\n'
            'Let\'s register you as a blood donor to help save lives!\n\n'
            'Please select your blood group:',
            reply_markup=reply_markup
        )
        return DIRECT_DONOR_BLOOD_GROUP


# Handle direct donor registration flow
async def direct_donor_blood_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store donor blood group and ask for division."""
    context.user_data['donor_blood_group'] = update.message.text

    # Create a keyboard with divisions
    keyboard = []
    for i in range(0, len(BANGLADESH_DIVISIONS), 2):
        row = BANGLADESH_DIVISIONS[i:i + 2]
        keyboard.append(row)

    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    await update.message.reply_text(
        'What is your division? Please select from the keyboard:',
        reply_markup=reply_markup
    )
    return DIRECT_DONOR_DIVISION


async def direct_donor_division(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store donor division and ask for district."""
    division = update.message.text
    context.user_data['donor_division'] = division

    # Get districts for this division
    if division in BANGLADESH_DIVISIONS:
        districts = BANGLADESH_DISTRICTS[division]

        # Create keyboard with districts
        keyboard = []
        for i in range(0, len(districts), 2):
            row = districts[i:i + 2]
            keyboard.append(row)

        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

        await update.message.reply_text(
            f'What is your district in {division}? Please select from the keyboard:',
            reply_markup=reply_markup
        )
    else:
        # Fallback if division is not in our list
        await update.message.reply_text('What is your district?')

    return DIRECT_DONOR_DISTRICT


async def direct_donor_district(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store donor district and complete the initial registration."""
    user_id = update.effective_user.id
    context.user_data['donor_district'] = update.message.text

    # Create minimal donor data (name and phone will be collected later)
    donor_data = {
        'telegram_id': user_id,
        'name': 'Not provided',  # Will be updated after request acceptance
        'age': 'Not provided',
        'phone': 'Not provided',  # Will be updated after request acceptance
        'district': context.user_data['donor_district'].strip().lower(),
        'division': context.user_data['donor_division'].strip().lower(),
        'area': context.user_data['donor_district'].strip().lower(),  # Use district as area initially
        'blood_group': context.user_data['donor_blood_group'],
        'gender': 'Not provided',
        'registration_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # Save donor to database
    donor_id = db.save_donor(donor_data)

    # Show main menu with options
    keyboard = [
        [InlineKeyboardButton("📊 Donor Dashboard", callback_data='open_donor_dashboard')],
        [InlineKeyboardButton("Request Blood", callback_data='request_blood')],
        [InlineKeyboardButton("View Donors", callback_data='view_donors')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f'✅ You are now registered as a blood donor!\n\n'
        f'Blood Group: {context.user_data["donor_blood_group"]}\n'
        f'Location: {context.user_data["donor_district"]}, {context.user_data["donor_division"]}\n\n'
        f'When someone in your area needs {context.user_data["donor_blood_group"]} blood, you will be notified.\n\n'
        f'Note: When you accept a donation request, you will need to provide your name and phone number.',
        reply_markup=reply_markup
    )

    # Find and show matching requests immediately
    donor = db.get_donor_by_telegram_id(user_id)
    if donor:
        await show_recent_matching_requests(update, context, donor)

    # Clear conversation data
    context.user_data.clear()
    return ConversationHandler.END

async def view_donors(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display a list of registered donors with limited information."""
    donors = db.get_all_donors()

    if not donors:
        if hasattr(update, 'callback_query'):
            await update.callback_query.message.reply_text("No donors registered yet.")
        else:
            await update.message.reply_text("No donors registered yet.")
        return

    # Privacy-focused donor list without contact details
    donor_list = "Registered Donors:\n\n"
    for donor in donors:
        donor_list += (
            f"Blood Group: {donor['blood_group']}\n"
            f"Location: {donor['area']}, {donor['district']}\n"
            f"---------------------\n"
        )

    # If the list is too long, split it into multiple messages
    if len(donor_list) > 4000:
        chunks = [donor_list[i:i + 4000] for i in range(0, len(donor_list), 4000)]
        for chunk in chunks:
            if hasattr(update, 'callback_query'):
                await update.callback_query.message.reply_text(chunk)
            else:
                await update.message.reply_text(chunk)
    else:
        if hasattr(update, 'callback_query'):
            await update.callback_query.message.reply_text(donor_list)
        else:
            await update.message.reply_text(donor_list)
# Add this function to show recent matching requests
async def show_recent_matching_requests(update: Update, context: ContextTypes.DEFAULT_TYPE, donor: dict) -> None:
    """Show recent blood requests matching donor's location and blood group."""
    try:
        # Get donor's blood group and location
        blood_group = donor['blood_group']
        division = donor['division'].strip().lower()
        district = donor['district'].strip().lower()

        # Get compatible blood groups for matching
        compatible_blood_groups = get_compatible_recipients(blood_group)

        # Find requests matching location and blood group
        matching_requests = []

        # First check for exact location match (same district)
        exact_match_requests = db.get_requests_by_location(division, district)
        for req in exact_match_requests:
            if req['blood_group'] in compatible_blood_groups:
                req['match_type'] = 'exact'
                matching_requests.append(req)

        # Then check division-level match if we don't have enough
        if len(matching_requests) < 3:
            division_match_requests = db.get_requests_by_location(division)
            for req in division_match_requests:
                if req['district'].strip().lower() != district and req['blood_group'] in compatible_blood_groups:
                    req['match_type'] = 'division'
                    matching_requests.append(req)

        # Limit to 3 recent requests
        matching_requests = matching_requests[:3]

        if matching_requests:
            await update.message.reply_text(
                "🩸 *RECENT BLOOD REQUESTS MATCHING YOUR PROFILE:*\n\n"
                "Here are recent blood requests you can help with:",
                parse_mode='Markdown'
            )

            # Show each matching request with accept option
            for req in matching_requests:
                match_label = "⭐ Exact location match" if req['match_type'] == 'exact' else "Near your division"

                message = (
                    f"🩸 *BLOOD NEEDED: {req['blood_group']}*\n\n"
                    f"*Hospital:* {req['hospital_name']}\n"
                    f"*Location:* {req['area']}, {req['district']}, {req['division']}\n"
                    f"*Urgency:* {req['urgency']}\n"
                    f"*When:* {req['request_date']}\n\n"
                    f"{match_label}"
                )

                keyboard = [
                    [InlineKeyboardButton("I Can Donate", callback_data=f"accept_{req['id']}_{donor['id']}")],
                    [InlineKeyboardButton("Not Available", callback_data=f"decline_{req['id']}_{donor['id']}")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                await update.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
        else:
            await update.message.reply_text(
                "No recent blood requests matching your profile at the moment.\n"
                "You will be notified when someone needs your help!"
            )

    except Exception as e:
        logger.error(f"Error showing recent matching requests: {e}")
        await update.message.reply_text("Error loading recent requests. Please try again later.")


# Function to determine compatible recipient blood groups for a donor
def get_compatible_recipients(donor_blood_group: str) -> list:
    """Return a list of recipient blood groups compatible with the donor's blood group."""
    compatibility = {
        'O-': ['O-', 'O+', 'A-', 'A+', 'B-', 'B+', 'AB-', 'AB+'],
        'O+': ['O+', 'A+', 'B+', 'AB+'],
        'A-': ['A-', 'A+', 'AB-', 'AB+'],
        'A+': ['A+', 'AB+'],
        'B-': ['B-', 'B+', 'AB-', 'AB+'],
        'B+': ['B+', 'AB+'],
        'AB-': ['AB-', 'AB+'],
        'AB+': ['AB+']
    }
    return compatibility.get(donor_blood_group, [])


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle button callbacks."""
    query = update.callback_query
    await query.answer()

    # Handle donor registration with terms & conditions (original flow)
    if query.data == 'register_donor':
        # Show terms and conditions first
        keyboard = [
            [InlineKeyboardButton("✅ I Agree", callback_data='accept_donor_terms')],
            [InlineKeyboardButton("❌ I Decline", callback_data='decline_donor_terms')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            DONOR_TERMS_TEXT,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return DONOR_TERMS_AGREEMENT

    # Handle terms and conditions response
    elif query.data == 'accept_donor_terms':
        # User agreed to terms, proceed with registration
        await query.edit_message_text(
            'Thank you for accepting the terms. Let\'s register you as a donor! What is your name?')
        return DONOR_NAME
    elif query.data == 'decline_donor_terms':
        # User declined terms
        await query.edit_message_text(
            'You have declined the terms and conditions. Unfortunately, you cannot register as a donor without accepting them.\n\n'
            'If you change your mind, you can start again with the /register command.'
        )
        return ConversationHandler.END

    # Handle donation terms response - CHECK THIS BEFORE THE GENERIC accept_ PATTERN
    elif query.data == 'accept_donation_terms':
        try:
            # User agreed to terms, check if name and phone are already provided
            donor_id = context.user_data.get('pending_accept_donor_id')
            donor = db.get_donor_by_id(donor_id)

            logger.info(f"User accepted donation terms, donor_id={donor_id}")

            if not donor:
                logger.error(f"Donor not found: {donor_id}")
                await query.message.reply_text("Error: Donor information not found.")
                return ConversationHandler.END

            # Send a new message instead of editing - avoids "message not modified" error
            if donor['name'] == 'Not provided' or donor['phone'] == 'Not provided':
                # Need to collect name and phone
                await query.message.reply_text(
                    "Thank you for agreeing to donate! Before we connect you with the recipient, "
                    "please provide your full name:"
                )
                logger.info(f"Asking for donor name, transitioning to DONOR_NAME_AFTER_ACCEPT")
                return DONOR_NAME_AFTER_ACCEPT
            else:
                # Donor already has complete info, proceed directly
                request_id = context.user_data.get('pending_accept_request_id')
                await query.message.reply_text("Processing your donation...")
                await handle_donation_acceptance(update, context, request_id, donor_id)
                return ConversationHandler.END
        except Exception as e:
            logger.error(f"Error in accept_donation_terms: {e}")
            await query.message.reply_text("Sorry, there was an error processing your donation acceptance.")
            return ConversationHandler.END

    elif query.data == 'decline_donation_terms':
        # User declined donation terms
        try:
            request_id = context.user_data.get('pending_accept_request_id')
            donor_id = context.user_data.get('pending_accept_donor_id')

            await query.edit_message_text(
                "You have declined the donation terms. Your donation has been cancelled.\n\n"
                "Thank you for considering to donate. You can always accept other requests in the future."
            )

            if request_id and donor_id:
                await handle_donation_decline(update, context, request_id, donor_id)
            return ConversationHandler.END
        except Exception as e:
            logger.error(f"Error in decline_donation_terms: {e}")
            await query.message.reply_text("Sorry, there was an error processing your response.")
            return ConversationHandler.END

    # Handle donation acceptance with agreement - AFTER the specific accept_donation_terms check
    elif query.data.startswith('accept_') and 'donation' not in query.data and 'donor' not in query.data:
        try:
            # Extract request_id and donor_id more safely
            parts = query.data.split('_')
            if len(parts) >= 3:
                request_id = parts[1]
                donor_id = parts[2]
            else:
                logger.error(f"Invalid callback data format: {query.data}")
                await query.message.reply_text("Error: Invalid callback data format.")
                return ConversationHandler.END

            # Store in context for later use
            context.user_data['pending_accept_request_id'] = request_id
            context.user_data['pending_accept_donor_id'] = donor_id

            logger.info(f"User accepting donation: request_id={request_id}, donor_id={donor_id}")

            # Show donation agreement terms
            keyboard = [
                [InlineKeyboardButton("✅ I Agree", callback_data='accept_donation_terms')],
                [InlineKeyboardButton("❌ I Decline", callback_data='decline_donation_terms')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                DONATION_TERMS_TEXT,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            return DONOR_TERMS_AFTER_ACCEPT
        except Exception as e:
            logger.error(f"Error in accept_ handler: {e}")
            await query.message.reply_text("Sorry, there was an error processing your donation acceptance.")
            return ConversationHandler.END


    elif query.data == 'request_blood':

        try:

            # Try to edit the message with new text

            await query.edit_message_text('Let\'s create a blood request. What is the patient\'s name?')

        except telegram.error.BadRequest as e:

            # If the error is about identical content, send a new message instead

            if "Message is not modified" in str(e):

                await query.message.reply_text('Let\'s create a blood request. What is the patient\'s name?')

            else:

                # For other types of BadRequest errors, re-raise

                raise

        return REQUEST_NAME
    elif query.data == 'view_donors':
        try:
            await view_donors(update, context)

            return ConversationHandler.END
        except Exception as e:
            logger.error(f"Error in view_donors handler: {e}")
            await query.message.reply_text("Error showing donors. Please try again later.")
            return ConversationHandler.END
    elif query.data == 'view_requests':
        # Only allow the admin to view all requests
        if update.effective_user.id == int(os.getenv('ADMIN_ID', '0')):
            try:
                await view_requests(update, context)
            except Exception as e:
                logger.error(f"Error in view_requests handler: {e}")
                await query.message.reply_text("Error showing requests. Please try again later.")
        else:
            await query.message.reply_text("Only administrators can view all requests.")
        return ConversationHandler.END
    elif query.data == 'open_admin_dashboard':
        # Open admin dashboard if user is admin
        if update.effective_user.id == int(os.getenv('ADMIN_ID', '0')):
            await admin_dashboard_message(update, context)
        else:
            await query.message.reply_text("Only administrators can access the dashboard.")
        return ConversationHandler.END
    elif query.data == 'open_donor_dashboard':
        # User wants to open their donor dashboard
        try:
            # Create a fake update with callback_query for the dashboard handler
            await refresh_donor_dashboard(update)
        except Exception as e:
            logger.error(f"Error opening donor dashboard: {e}")
            await query.message.reply_text("Error opening donor dashboard. Please try again later.")
        return ConversationHandler.END

    # Handle donation decline - also check pattern more specifically
    elif query.data.startswith('decline_') and 'donation' not in query.data and 'donor' not in query.data:
        # Handle donation decline
        try:
            parts = query.data.split('_')
            if len(parts) >= 3:
                request_id = parts[1]
                donor_id = parts[2]
                await handle_donation_decline(update, context, request_id, donor_id)
            else:
                logger.error(f"Invalid decline callback format: {query.data}")
                await query.message.reply_text("Error: Invalid callback data format.")
            return ConversationHandler.END
        except Exception as e:
            logger.error(f"Error handling decline: {e}")
            await query.message.reply_text("Sorry, there was an error processing your response.")
            return ConversationHandler.END

    return ConversationHandler.END

# Handle donor name collection after accepting a request
async def donor_name_after_accept(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Collect donor name after accepting a donation request."""
    try:
        logger.info(f"Processing donor name after accept: {update.message.text}")
        context.user_data['donor_name'] = update.message.text

        await update.message.reply_text("Please provide your phone number so the recipient can contact you:")
        return DONOR_PHONE_AFTER_ACCEPT
    except Exception as e:
        logger.error(f"Error in donor_name_after_accept: {e}")
        await update.message.reply_text(
            "Sorry, there was an error processing your name. Please try again or contact support."
        )
        return ConversationHandler.END


async def donor_phone_after_accept(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Collect donor phone after accepting a donation request and complete the process."""
    try:
        logger.info(f"Processing donor phone after accept")
        context.user_data['donor_phone'] = update.message.text

        request_id = context.user_data.get('pending_accept_request_id')
        donor_id = context.user_data.get('pending_accept_donor_id')

        if not request_id or not donor_id:
            logger.error(f"Missing request_id or donor_id in context: {context.user_data}")
            await update.message.reply_text("Sorry, there was an error with your donation acceptance.")
            return ConversationHandler.END

        # Update donor information in the database
        update_data = {
            'name': context.user_data['donor_name'],
            'phone': context.user_data['donor_phone']
        }
        db.update_donor(donor_id, update_data)

        # Now proceed with the donation acceptance
        try:
            await handle_donation_acceptance(update, context, request_id, donor_id)
        except Exception as inner_e:
            logger.error(f"Error in handle_donation_acceptance: {inner_e}")
            # If the donation was processed but there was an error with the UI, we can still inform the user
            await update.message.reply_text(
                "Your donation has been accepted and recorded, but there was an error creating the chat group. "
                "The recipient has been notified of your details."
            )

        # Clear conversation data
        context.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in donor_phone_after_accept: {e}")
        await update.message.reply_text(
            "Sorry, there was an error processing your phone number. Please try again or contact support."
        )
        return ConversationHandler.END

# =================== DONOR REGISTRATION FUNCTIONS (ORIGINAL FLOW) ===================

async def donor_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store donor name and ask for age."""
    context.user_data['donor_name'] = update.message.text
    await update.message.reply_text('What is your age?')
    return DONOR_AGE


async def donor_age(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store donor age and ask for phone number."""
    context.user_data['donor_age'] = update.message.text
    await update.message.reply_text('What is your phone number?')
    return DONOR_PHONE


async def donor_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store donor phone and ask for division."""
    context.user_data['donor_phone'] = update.message.text

    # Create a keyboard with divisions
    keyboard = []
    for i in range(0, len(BANGLADESH_DIVISIONS), 2):
        row = BANGLADESH_DIVISIONS[i:i + 2]
        keyboard.append(row)

    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    await update.message.reply_text(
        'What is your division? Please select from the keyboard:',
        reply_markup=reply_markup
    )
    return DONOR_DIVISION


async def donor_division(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store donor division and ask for district based on division."""
    division = update.message.text
    context.user_data['donor_division'] = division

    # Get districts for this division
    if division in BANGLADESH_DIVISIONS:
        districts = BANGLADESH_DISTRICTS[division]

        # Create keyboard with districts
        keyboard = []
        for i in range(0, len(districts), 2):
            row = districts[i:i + 2]
            keyboard.append(row)

        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

        await update.message.reply_text(
            f'What is your district in {division}? Please select from the keyboard:',
            reply_markup=reply_markup
        )
    else:
        # Fallback if division is not in our list
        await update.message.reply_text('What is your district?')

    return DONOR_DISTRICT


async def donor_district(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store donor district and ask for specific area."""
    context.user_data['donor_district'] = update.message.text
    await update.message.reply_text('What is your specific area?')
    return DONOR_AREA


async def donor_area(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store donor area and ask for blood group."""
    context.user_data['donor_area'] = update.message.text

    keyboard = [
        ['A+', 'A-', 'B+', 'B-'],
        ['AB+', 'AB-', 'O+', 'O-']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)

    await update.message.reply_text('What is your blood group?', reply_markup=reply_markup)
    return DONOR_BLOOD_GROUP


async def donor_blood_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store donor blood group and ask for gender."""
    context.user_data['donor_blood_group'] = update.message.text

    keyboard = [['Male', 'Female', 'Other']]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)

    await update.message.reply_text('What is your gender?', reply_markup=reply_markup)
    return DONOR_GENDER


async def donor_gender(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store donor gender and complete registration."""
    user_id = update.effective_user.id
    context.user_data['donor_gender'] = update.message.text

    # Create donor data
    donor_data = {
        'telegram_id': user_id,
        'name': context.user_data['donor_name'],
        'age': context.user_data['donor_age'],
        'phone': context.user_data['donor_phone'],
        'district': context.user_data['donor_district'].strip().lower(),
        'division': context.user_data['donor_division'].strip().lower(),
        'area': context.user_data['donor_area'],
        'blood_group': context.user_data['donor_blood_group'],
        'gender': context.user_data['donor_gender'],
        'registration_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # Save donor to database
    donor_id = db.save_donor(donor_data)

    await update.message.reply_text(
        f'Member registration completed successfully!\n\n'
        f'Thank you for registering as a member, {context.user_data["donor_name"]}!\n'
        f'When you will be in need of  {context.user_data["donor_blood_group"]} blood, we are here to help you out.'
        f'On the other hand,when someone near you needs {context.user_data["donor_blood_group"]} blood, you will be notified.'
    )

    # Clear conversation data
    context.user_data.clear()
    return ConversationHandler.END


# =================== BLOOD REQUEST FUNCTIONS ===================

async def request_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store request name and ask for age."""
    context.user_data['request_name'] = update.message.text
    await update.message.reply_text('What is the patient\'s age?')
    return REQUEST_AGE


async def request_age(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store request age and ask for hospital name."""
    context.user_data['request_age'] = update.message.text
    await update.message.reply_text('What is the hospital name?')
    return REQUEST_HOSPITAL_NAME


async def request_hospital_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store hospital name and ask for address."""
    context.user_data['request_hospital_name'] = update.message.text
    await update.message.reply_text('What is the hospital address?')
    return REQUEST_HOSPITAL_ADDRESS


async def request_hospital_address(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store hospital address and ask for area."""
    context.user_data['request_hospital_address'] = update.message.text
    await update.message.reply_text('What is the area?')
    return REQUEST_AREA


async def request_area(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store area and ask for division with keyboard."""
    context.user_data['request_area'] = update.message.text

    # Create a keyboard with divisions
    keyboard = []
    for i in range(0, len(BANGLADESH_DIVISIONS), 2):
        row = BANGLADESH_DIVISIONS[i:i + 2]
        keyboard.append(row)

    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    await update.message.reply_text(
        'What is the division? Please select from the keyboard:',
        reply_markup=reply_markup
    )
    return REQUEST_DIVISION


async def request_division(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store division and ask for district with keyboard based on division."""
    division = update.message.text
    context.user_data['request_division'] = division

    # Get districts for this division
    if division in BANGLADESH_DIVISIONS:
        districts = BANGLADESH_DISTRICTS[division]

        # Create keyboard with districts
        keyboard = []
        for i in range(0, len(districts), 2):
            row = districts[i:i + 2]
            keyboard.append(row)

        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

        await update.message.reply_text(
            f'What is the district in {division}? Please select from the keyboard:',
            reply_markup=reply_markup
        )
    else:
        # Fallback if division is not in our list
        await update.message.reply_text('What is the district?')

    return REQUEST_DISTRICT


async def request_district(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store district and ask for urgency level."""
    context.user_data['request_district'] = update.message.text

    keyboard = [['Urgent', 'High', 'Medium', 'Low']]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)

    await update.message.reply_text('What is the urgency level?', reply_markup=reply_markup)
    return REQUEST_URGENCY


async def request_urgency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store urgency and ask for phone number."""
    context.user_data['request_urgency'] = update.message.text
    await update.message.reply_text('What is your phone number?')
    return REQUEST_PHONE


async def request_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store phone number and ask for blood group."""
    context.user_data['request_phone'] = update.message.text

    keyboard = [
        ['A+', 'A-', 'B+', 'B-'],
        ['AB+', 'AB-', 'O+', 'O-']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)

    await update.message.reply_text('What blood group is needed?', reply_markup=reply_markup)
    return REQUEST_BLOOD_GROUP


async def request_blood_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store blood group and complete request."""
    user_id = update.effective_user.id
    context.user_data['request_blood_group'] = update.message.text

    # Create request data
    request_data = {
        'telegram_id': user_id,
        'name': context.user_data['request_name'],
        'age': context.user_data['request_age'],
        'hospital_name': context.user_data['request_hospital_name'],
        'hospital_address': context.user_data['request_hospital_address'],
        'area': context.user_data['request_area'],
        'division': context.user_data['request_division'].strip().lower(),
        'district': context.user_data['request_district'].strip().lower(),
        'urgency': context.user_data['request_urgency'],
        'phone': context.user_data['request_phone'],
        'blood_group': context.user_data['request_blood_group'],
        'request_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'status': 'active'  # Adding default status
    }

    # Save request to database
    request_id = db.save_request(request_data)

    await update.message.reply_text(
        f'Your blood request has been submitted successfully!\n\n'
        f'Donors with blood group {request_data["blood_group"]} in your area will be notified.'
    )

    # Find and notify matching donors
    await find_matching_donors(context, str(request_id))

    # Clear conversation data
    context.user_data.clear()
    return ConversationHandler.END


async def find_matching_donors(context: ContextTypes.DEFAULT_TYPE, request_id: str) -> None:
    """Find donors that match the blood group and location and notify them."""
    # Debug log start of function
    logger.info(f"Starting donor matching process for request {request_id}")

    request = db.get_request_by_id(request_id)
    if not request:
        logger.error(f"Request with ID {request_id} not found!")
        return

    blood_group = request.get('blood_group', '')
    division = request.get('division', '').strip().lower()
    district = request.get('district', '').strip().lower()

    if not blood_group:
        logger.error(f"Request {request_id} has no blood_group!")
        return

    logger.info(f"Request details: Blood Group={blood_group}, Division={division}, District={district}")

    # Define compatible blood groups
    compatible_blood_groups = get_compatible_donors(blood_group)
    logger.info(f"Compatible blood groups: {compatible_blood_groups}")

    # Find donors matching blood group
    all_compatible_donors = db.get_donors_by_blood_groups(compatible_blood_groups)
    logger.info(f"Found {len(all_compatible_donors)} donors with compatible blood groups")

    if not all_compatible_donors:
        logger.info(f"No compatible donors found for blood group {blood_group}")
        return

    # Categorize donors by location match
    exact_match_donors = []  # Same district and division
    division_match_donors = []  # Same division, different district
    blood_only_match_donors = []  # Just blood type match, different location

    for donor in all_compatible_donors:
        donor_division = donor.get('division', '').strip().lower()
        donor_district = donor.get('district', '').strip().lower()
        donor_id = donor.get('id', 'unknown')
        donor_tg_id = donor.get('telegram_id', 'unknown')

        logger.debug(
            f"Checking donor ID={donor_id}, TG_ID={donor_tg_id}, Division={donor_division}, District={donor_district}")

        # Check for exact location match (same district and division)
        if donor_division == division and donor_district == district:
            exact_match_donors.append(donor)
            logger.debug(f"Added donor {donor_id} to exact match list")
        # Check for division match only
        elif donor_division == division:
            division_match_donors.append(donor)
            logger.debug(f"Added donor {donor_id} to division match list")
        # All others are blood-only matches
        else:
            blood_only_match_donors.append(donor)
            logger.debug(f"Added donor {donor_id} to blood-only match list")

    # Create prioritized list: exact matches first, then division matches, then blood-only matches
    matching_donors = exact_match_donors + division_match_donors + blood_only_match_donors

    logger.info(f"Matching donors categorized: {len(exact_match_donors)} exact matches, "
                f"{len(division_match_donors)} division matches, and "
                f"{len(blood_only_match_donors)} blood-only matches")

    if not matching_donors:
        logger.info(f"No donors found after location filtering for request {request_id}")
        return

    # Track notified donors to update in the database
    notified_donors = []

    # Notify donors
    logger.info(f"Starting notification process for {len(matching_donors)} donors")
    for i, donor in enumerate(matching_donors):
        try:
            donor_id = str(donor.get('id', ''))
            donor_tg_id = donor.get('telegram_id')

            if not donor_id or not donor_tg_id:
                logger.warning(f"Skipping donor with missing ID or Telegram ID: {donor}")
                continue

            logger.info(f"Processing donor {i + 1}/{len(matching_donors)}: ID={donor_id}, TG_ID={donor_tg_id}")

            # Determine match type for the message
            match_type = ""
            if donor in exact_match_donors:
                match_type = "⭐ This request is from your exact location (same district)"
            elif donor in division_match_donors:
                match_type = "✨ This request is from your division"

            # Create a message with limited information - no patient name or contact info
            # Use .get() with default values to avoid KeyError
            message = (
                f"🩸 URGENT: Blood Donation Request\n\n"
                f"A patient needs {blood_group} blood donation\n"
                f"Hospital: {request.get('hospital_name', 'Not specified')}\n"
                f"Location: {request.get('area', 'Not specified')}, {request.get('district', 'Not specified')}, {request.get('division', 'Not specified')}\n"
            )

            # Add urgency if available
            if 'urgency' in request:
                message += f"Urgency: {request['urgency']}\n\n"
            else:
                message += "Urgency: High\n\n"  # Default urgency

            message += f"You are receiving this notification because your blood group ({donor.get('blood_group', '')}) is compatible."

            # Add location match note if applicable
            if match_type:
                message += f"\n\n{match_type}"

            # Create accept button with callback data
            keyboard = [
                [InlineKeyboardButton("I Can Donate", callback_data=f"accept_{request_id}_{donor_id}")],
                [InlineKeyboardButton("Not Available", callback_data=f"decline_{request_id}_{donor_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            logger.info(f"Sending notification to donor {donor_id} (TG_ID: {donor_tg_id})")

            # Send notification to donor with buttons
            await context.bot.send_message(
                chat_id=donor_tg_id,  # Using the telegram_id directly
                text=message,
                reply_markup=reply_markup
            )

            logger.info(f"Successfully sent notification to donor {donor_id}")

            # Add donor to notified list
            notified_donors.append(donor_id)

        except Exception as e:
            logger.error(f"Failed to notify donor {donor.get('id', 'unknown')}: {e}")
            # Print the full error traceback for detailed debugging
            import traceback
            logger.error(traceback.format_exc())

    # Update request with the list of notified donors
    if notified_donors:
        logger.info(f"Updating request {request_id} with {len(notified_donors)} notified donors")
        success = db.update_request_notified_donors(request_id, notified_donors)
        logger.info(f"Database update {'successful' if success else 'failed'}")
    else:
        logger.warning(f"No donors were successfully notified for request {request_id}")


def get_compatible_donors(requested_blood_group: str) -> list:
    """Return a list of compatible donor blood groups for the requested blood group."""
    compatibility = {
        'A+': ['A+', 'A-', 'O+', 'O-'],
        'A-': ['A-', 'O-'],
        'B+': ['B+', 'B-', 'O+', 'O-'],
        'B-': ['B-', 'O-'],
        'AB+': ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-'],
        'AB-': ['A-', 'B-', 'AB-', 'O-'],
        'O+': ['O+', 'O-'],
        'O-': ['O-']
    }
    return compatibility.get(requested_blood_group, [])


def get_total_successful_operations() -> int:
    """Get the total number of successful donation operations."""
    try:
        # Use database function to get operations stats
        stats = db.get_operations_stats()
        return stats.get('total_operations', 0)
    except Exception as e:
        logger.error(f"Error counting successful operations: {e}")
        return 0

async def handle_donation_decline(update: Update, context: ContextTypes.DEFAULT_TYPE, request_id: str,
                                  donor_id: str) -> None:
    """Handle when a donor declines a blood donation request."""
    query = update.callback_query

    # Update message to show decline
    await query.edit_message_text(
        "You have declined this blood donation request. Thank you for considering.\n\n"
        "You can still donate in the future if your availability changes.",
        reply_markup=None
    )

    # Record the decline in the database
    db.add_donor_to_declined_request(request_id, donor_id)


async def handle_donation_acceptance(update: Update, context: ContextTypes.DEFAULT_TYPE, request_id: str, donor_id: str) -> None:

    donor = db.get_donor_by_id(donor_id)
    request = db.get_request_by_id(request_id)

    if not donor or not request:
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.message.reply_text("Sorry, the donation request is no longer valid.")
        else:
            await update.message.reply_text("Sorry, the donation request is no longer valid.")
        return

    # Update the request to record the donor's acceptance
    db.add_donor_to_request(request_id, donor_id)

    # Get usernames if available
    donor_username = None
    requester_username = None

    try:
        # Try to get donor's username
        donor_user = await context.bot.get_chat(donor['telegram_id'])
        donor_username = donor_user.username
    except Exception as e:
        logger.error(f"Error getting donor username: {e}")

    try:
        # Try to get requester's username
        requester_user = await context.bot.get_chat(request['telegram_id'])
        requester_username = requester_user.username
    except Exception as e:
        logger.error(f"Error getting requester username: {e}")

    # Notify the donor with complete request details including contact info
    donor_msg = (
        f"Thank you for accepting this donation request!\n\n"
        f"Patient Details:\n"
        f"Name: {request['name']}\n"
        f"Age: {request['age']}\n"
        f"Blood Group: {request['blood_group']}\n"
        f"Hospital: {request['hospital_name']}\n"
        f"Address: {request['hospital_address']}\n"
        f"Contact: {request['phone']}\n\n"
    )

    # Add requester username if available
    if requester_username:
        donor_msg += f"You can contact the requester directly on Telegram: @{requester_username}\n"
    else:
        donor_msg += f"Please contact them directly using the phone number provided above.\n"

    # Create feedback keyboard for donor
    donor_feedback_keyboard = [
        [InlineKeyboardButton("📝 Share Donation Experience", callback_data='open_support')],
        [InlineKeyboardButton("📊 View My Dashboard", callback_data='open_donor_dashboard')]
    ]
    donor_reply_markup = InlineKeyboardMarkup(donor_feedback_keyboard)

    # Use the correct method to send the message based on update type
    if hasattr(update, 'callback_query') and update.callback_query:
        try:
            await update.callback_query.edit_message_text(donor_msg, reply_markup=None)
            # Send a follow-up message with the feedback buttons
            await update.callback_query.message.reply_text(
                "After your donation, we'd love to hear about your experience!",
                reply_markup=donor_reply_markup
            )
        except Exception as e:
            logger.error(f"Error editing message: {e}")
            await update.callback_query.message.reply_text(
                donor_msg,
                reply_markup=donor_reply_markup
            )
    else:
        await update.message.reply_text(
            donor_msg,
            reply_markup=donor_reply_markup
        )

    # Create feedback keyboard for requester
    requester_feedback_keyboard = [
        [InlineKeyboardButton("📝 Share Feedback About Donor", callback_data='open_support')],
        [InlineKeyboardButton("🙏 Thank You Message", callback_data='send_thanks')]
    ]
    requester_reply_markup = InlineKeyboardMarkup(requester_feedback_keyboard)

    # Notify the requester
    requester_msg = (
        f"Good news! A donor has accepted your blood request.\n\n"
        f"Donor Details:\n"
        f"Name: {donor['name']}\n"
        f"Blood Group: {donor['blood_group']}\n"
        f"Contact: {donor['phone']}\n\n"
    )

    # Add donor username if available
    if donor_username:
        requester_msg += f"You can contact the donor directly on Telegram: @{donor_username}\n"
    else:
        requester_msg += f"Please contact the donor using the phone number provided above.\n"

    try:
        await context.bot.send_message(
            chat_id=request['telegram_id'],
            text=requester_msg,
            reply_markup=requester_reply_markup
        )

        try:
            # Get admin ID from environment variables
            admin_id = os.getenv('ADMIN_ID', '0')

            # Get total operations count
            total_operations = get_total_successful_operations()

            # Create a detailed admin notification
            admin_msg = (
                f"🎉 *SUCCESSFUL DONATION OPERATION #{total_operations}*\n\n"
                f"*DONOR DETAILS:*\n"
                f"ID: `{donor['id']}`\n"
                f"Name: {donor['name']}\n"
                f"Age: {donor['age']}\n"
                f"Gender: {donor['gender']}\n"
                f"Blood Group: {donor['blood_group']}\n"
                f"Phone: `{donor['phone']}`\n"
                f"Location: {donor['area']}, {donor['district']}, {donor['division']}\n"
                f"Username: {f'@{donor_username}' if donor_username else 'Not available'}\n\n"

                f"*RECIPIENT DETAILS:*\n"
                f"ID: `{request['id']}`\n"
                f"Patient: {request['name']}\n"
                f"Age: {request['age']}\n"
                f"Blood Group: {request['blood_group']}\n"
                f"Hospital: {request['hospital_name']}\n"
                f"Address: {request['hospital_address']}\n"
                f"Urgency: {request['urgency']}\n"
                f"Phone: `{request['phone']}`\n"
                f"Username: {f'@{requester_username}' if requester_username else 'Not available'}\n\n"

                f"*OPERATION DETAILS:*\n"
                f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Status: Donor has accepted request\n\n"
                f"Total successful operations to date: {total_operations}"
            )

            # Send notification to admin
            await context.bot.send_message(
                chat_id=admin_id,
                text=admin_msg,
                parse_mode='Markdown'
            )

            # Log the successful notification
            logger.info(f"Admin notified about donation operation #{total_operations}")
        except Exception as e:
            logger.error(f"Error notifying admin about donation: {e}")
    except Exception as e:
        logger.error(f"Error notifying requester: {e}")

async def send_thanks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Send a thank you message to the donor."""
    query = update.callback_query
    await query.answer()

    await query.message.reply_text(
        "📝 *THANK YOU MESSAGE*\n\n"
        "Please write a thank you message for the donor who helped you. "
        "This message will be sent directly to them.\n\n"
        "Type your message now or use /cancel to abort.",
        parse_mode='Markdown'
    )
    return SUPPORT_MESSAGE
async def view_requests(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display a list of active blood requests - ADMIN ONLY."""
    active_requests = db.get_active_requests()

    if not active_requests:
        if hasattr(update, 'callback_query'):
            await update.callback_query.message.reply_text("No active blood requests at the moment.")
        else:
            await update.message.reply_text("No active blood requests at the moment.")
        return

    request_list = "Active Blood Requests:\n\n"
    for req in active_requests:
        request_list += (
            f"ID: {req['id']}\n"
            f"Patient: {req['name']}, {req['age']}\n"
            f"Blood Group: {req['blood_group']}\n"
            f"Hospital: {req['hospital_name']}\n"
            f"Urgency: {req['urgency']}\n"
            f"Contact: {req['phone']}\n"
            f"---------------------\n"
        )

    # If the list is too long, split it into multiple messages
    if len(request_list) > 4000:
        chunks = [request_list[i:i + 4000] for i in range(0, len(request_list), 4000)]
        for chunk in chunks:
            if hasattr(update, 'callback_query'):
                await update.callback_query.message.reply_text(chunk)
            else:
                await update.message.reply_text(chunk)
    else:
        if hasattr(update, 'callback_query'):
            await update.callback_query.message.reply_text(request_list)
        else:
            await update.message.reply_text(request_list)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""
    # Check if user is a donor
    user_id = update.effective_user.id
    donor = db.get_donor_by_telegram_id(user_id)

    help_text = (
        "🩸 *Blood Donation Bot - Help*\n\n"
        "*/start* - Start the bot and register as donor\n"
        "*/request* - Create a blood donation request\n"
        "*/donors* - View registered donors\n"
        "*/help* - Show this help message\n"
    )

    # Add donor commands if the user is a registered donor
    if donor:
        donor_help = (
            "\n*Donor Commands:*\n"
            "*/mydashboard* - View your donor dashboard with stats\n"
            "*/mystats* - Same as /mydashboard\n"
        )
        help_text += donor_help

    # Add admin commands if the user is an admin
    if update.effective_user.id == int(os.getenv('ADMIN_ID', '0')):
        admin_help = (
            "\n👑 *Admin Commands:*\n"
            "*/admin* - Open admin dashboard with interactive buttons\n"
            "*/dashboard* - Same as /admin - opens the admin panel\n"
            "*/requests* - View all active blood requests\n"
            "*/stats* - View donation operation statistics\n"
            "*/operations* - List all successful donations\n"
        )
        help_text += admin_help

    await update.message.reply_text(help_text, parse_mode='Markdown')


async def register_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start donor registration process with terms and conditions."""
    # Show terms and conditions first
    keyboard = [
        [InlineKeyboardButton("✅ I Agree", callback_data='accept_donor_terms')],
        [InlineKeyboardButton("❌ I Decline", callback_data='decline_donor_terms')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        DONOR_TERMS_TEXT,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    return DONOR_TERMS_AGREEMENT


async def handle_terms_response(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the user's response to the terms and conditions."""
    query = update.callback_query
    await query.answer()

    if query.data == 'accept_donor_terms':
        # User agreed to terms, proceed with registration
        await query.edit_message_text(
            'Thank you for accepting the terms. Let\'s register you as a donor! What is your name?')
        return DONOR_NAME
    else:
        # User declined terms
        await query.edit_message_text(
            'You have declined the terms and conditions. Unfortunately, you cannot register as a donor without accepting them.\n\n'
            'If you change your mind, you can use the /register command again.'
        )
        return ConversationHandler.END


async def donors_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show registered donors with limited information."""
    donors = db.get_all_donors()

    if not donors:
        await update.message.reply_text("No donors registered yet.")
        return

    # Privacy-focused donor list without contact details
    donor_list = "Registered Donors:\n\n"
    for donor in donors:
        donor_list += (
            f"Blood Group: {donor['blood_group']}\n"
            f"Location: {donor['area']}, {donor['district']}\n"
            f"---------------------\n"
        )

    # If the list is too long, split it into multiple messages
    if len(donor_list) > 4000:
        chunks = [donor_list[i:i + 4000] for i in range(0, len(donor_list), 4000)]
        for chunk in chunks:
            await update.message.reply_text(chunk)
    else:
        await update.message.reply_text(donor_list)


async def requests_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show active blood requests - for ADMIN only."""
    try:
        admin_id = int(os.getenv('ADMIN_ID', '0').strip())
        user_id = update.effective_user.id

        if user_id == admin_id:
            active_requests = db.get_active_requests()

            if not active_requests:
                await update.message.reply_text("No active blood requests at the moment.")
                return

            request_list = "🩸 *Active Blood Requests:*\n\n"
            for req in active_requests:
                request_list += (
                    f"*ID:* `{req['id']}`\n"
                    f"*Patient:* {req['name']}, {req['age']} yrs\n"
                    f"*Blood Group:* {req['blood_group']}\n"
                    f"*Hospital:* {req['hospital_name']}\n"
                    f"*Urgency:* {req['urgency']}\n"
                    f"*Contact:* `{req['phone']}`\n"
                    f"-------------------------\n"
                )

            # If too long, send in chunks
            if len(request_list) > 4000:
                chunks = [request_list[i:i + 4000] for i in range(0, len(request_list), 4000)]
                for chunk in chunks:
                    await update.message.reply_text(chunk, parse_mode='Markdown')
            else:
                await update.message.reply_text(request_list, parse_mode='Markdown')

        else:
            await update.message.reply_text("⛔ Only administrators can view all requests.")

    except Exception as e:
        await update.message.reply_text(f"⚠️ Error: {e}")


async def donor_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display personalized donor dashboard with statistics and rankings."""
    try:
        user_id = update.effective_user.id
        user = update.effective_user

        # Find the donor by telegram ID
        donor = db.get_donor_by_telegram_id(user_id)

        if not donor:
            # User is not registered as a donor
            keyboard = [
                [InlineKeyboardButton("Register as Donor", callback_data='register_donor')],
                [InlineKeyboardButton("📱 Main Menu", callback_data='show_main_menu')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                "📊 *DONOR DASHBOARD*\n\n"
                "You are not registered as a donor yet. Register to track your donations and see your ranking!\n\n"
                "By registering, you can help save lives and see your impact in the community.",
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            return

        # Get donor statistics
        donor_stats = db.get_donor_stats(donor['id'])

        if not donor_stats:
            await update.message.reply_text("Error retrieving your donor statistics. Please try again later.")
            return

        # Get top donors of all time
        top_donors_all_time = db.get_top_donors(3)

        # Get top donors of this month
        top_donors_month = db.get_top_donors(3, 'month')

        # Create the dashboard message
        dashboard_msg = (
            f"🩸 *DONOR DASHBOARD*\n\n"
            f"Hello, {donor['name']}!\n\n"

            f"*YOUR STATISTICS:*\n"
            f"Blood Group: {donor['blood_group']}\n"
            f"Total Donations: {donor_stats['total_donations']}\n"
            f"Fulfilled Donations: {donor_stats['fulfilled_donations']}\n"
            f"Pending Donations: {donor_stats['pending_donations']}\n"
        )

        # Add ranking information if available
        if donor_stats['donor_rank']:
            dashboard_msg += f"Your Ranking: #{donor_stats['donor_rank']} among all donors\n"

        dashboard_msg += "\n*TOP DONORS OF ALL TIME:*\n"

        # Add top donors of all time
        if top_donors_all_time:
            for i, top_donor in enumerate(top_donors_all_time, 1):
                # Highlight if this is the current donor
                if str(top_donor['id']) == str(donor['id']):
                    dashboard_msg += f"{i}. 🌟 *{top_donor['name']}* - {top_donor['donation_count']} donations\n"
                else:
                    dashboard_msg += f"{i}. {top_donor['name']} - {top_donor['donation_count']} donations\n"
        else:
            dashboard_msg += "No donations recorded yet.\n"

        dashboard_msg += "\n*TOP DONORS THIS MONTH:*\n"

        # Add top donors of this month
        if top_donors_month:
            for i, top_donor in enumerate(top_donors_month, 1):
                # Highlight if this is the current donor
                if str(top_donor['id']) == str(donor['id']):
                    dashboard_msg += f"{i}. 🌟 *{top_donor['name']}* - {top_donor['donation_count']} donations\n"
                else:
                    dashboard_msg += f"{i}. {top_donor['name']} - {top_donor['donation_count']} donations\n"
        else:
            dashboard_msg += "No donations recorded this month.\n"

        # Add motivational message
        dashboard_msg += (
            "\n*IMPACT:*\n"
            "Each donation can save up to 3 lives! "
            f"You've potentially saved {donor_stats['fulfilled_donations'] * 3} lives with your donations.\n\n"
            "Thank you for being a lifesaver! 💖"
        )

        # Add buttons for actions - using the correct callback data
        keyboard = [
            [InlineKeyboardButton("🔄 Refresh Dashboard", callback_data='refresh_donor_dashboard')],
            [InlineKeyboardButton("📱 Main Menu", callback_data='show_main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            dashboard_msg,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

    except Exception as e:
        logger.error(f"Error in donor dashboard: {e}")
        await update.message.reply_text(f"Error loading donor dashboard: {str(e)}")


async def refresh_donor_dashboard(update: Update) -> None:
    """Refresh the donor dashboard with latest statistics."""
    query = update.callback_query
    user_id = update.effective_user.id

    try:
        # Find the donor by telegram ID
        donor = db.get_donor_by_telegram_id(user_id)

        if not donor:
            # User is not registered as a donor
            keyboard = [
                [InlineKeyboardButton("Register as Donor", callback_data='register_donor')],
                [InlineKeyboardButton("📱 Main Menu", callback_data='show_main_menu')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                "📊 *DONOR DASHBOARD*\n\n"
                "You are not registered as a donor yet. Register to track your donations and see your ranking!\n\n"
                "By registering, you can help save lives and see your impact in the community.",
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            return

        # Get donor statistics
        donor_stats = db.get_donor_stats(donor['id'])

        if not donor_stats:
            keyboard = [[InlineKeyboardButton("📱 Main Menu", callback_data='show_main_menu')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                "Error retrieving your donor statistics. Please try again later.",
                reply_markup=reply_markup
            )
            return

        # Get top donors of all time
        top_donors_all_time = db.get_top_donors(3)

        # Get top donors of this month
        top_donors_month = db.get_top_donors(3, 'month')

        # Create the dashboard message
        dashboard_msg = (
            f"🩸 *DONOR DASHBOARD*\n\n"
            f"Hello, {donor['name']}!\n\n"

            f"*YOUR STATISTICS:*\n"
            f"Blood Group: {donor['blood_group']}\n"
            f"Total Donations: {donor_stats['total_donations']}\n"
            f"Fulfilled Donations: {donor_stats['fulfilled_donations']}\n"
            f"Pending Donations: {donor_stats['pending_donations']}\n"
        )

        # Add ranking information if available
        if donor_stats['donor_rank']:
            dashboard_msg += f"Your Ranking: #{donor_stats['donor_rank']} among all donors\n"

        dashboard_msg += "\n*TOP DONORS OF ALL TIME:*\n"

        # Add top donors of all time
        if top_donors_all_time:
            for i, top_donor in enumerate(top_donors_all_time, 1):
                # Highlight if this is the current donor
                if str(top_donor['id']) == str(donor['id']):
                    dashboard_msg += f"{i}. 🌟 *{top_donor['name']}* - {top_donor['donation_count']} donations\n"
                else:
                    dashboard_msg += f"{i}. {top_donor['name']} - {top_donor['donation_count']} donations\n"
        else:
            dashboard_msg += "No donations recorded yet.\n"

        dashboard_msg += "\n*TOP DONORS THIS MONTH:*\n"

        # Add top donors of this month
        if top_donors_month:
            for i, top_donor in enumerate(top_donors_month, 1):
                # Highlight if this is the current donor
                if str(top_donor['id']) == str(donor['id']):
                    dashboard_msg += f"{i}. 🌟 *{top_donor['name']}* - {top_donor['donation_count']} donations\n"
                else:
                    dashboard_msg += f"{i}. {top_donor['name']} - {top_donor['donation_count']} donations\n"
        else:
            dashboard_msg += "No donations recorded this month.\n"

        # Add motivational message
        dashboard_msg += (
            "\n*IMPACT:*\n"
            "Each donation can save up to 3 lives! "
            f"You've potentially saved {donor_stats['fulfilled_donations'] * 3} lives with your donations.\n\n"
            "Thank you for being a lifesaver! 💖"
        )

        # Add buttons for actions
        keyboard = [
            [InlineKeyboardButton("🔄 Refresh Dashboard", callback_data='refresh_donor_dashboard')],
            [InlineKeyboardButton("📱 Main Menu", callback_data='show_main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            dashboard_msg,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

    except Exception as e:
        logger.error(f"Error refreshing donor dashboard: {e}")

        # Try to show an error message
        try:
            keyboard = [[InlineKeyboardButton("📱 Main Menu", callback_data='show_main_menu')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                f"Error refreshing donor dashboard: {str(e)}\n\nPlease try again or return to the main menu.",
                reply_markup=reply_markup
            )
        except Exception as inner_e:
            logger.error(f"Error showing error message: {inner_e}")


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the main menu of the bot."""
    query = update.callback_query
    await query.answer()

    user = update.effective_user

    try:
        # Check if user is already registered as a donor
        donor = db.get_donor_by_telegram_id(user.id)

        # Default keyboard for all users
        keyboard = [
            [InlineKeyboardButton("Request Blood", callback_data='request_blood')],
            [InlineKeyboardButton("View Donors", callback_data='view_donors')],
            [InlineKeyboardButton("📬 Contact Support", callback_data='open_support')],

        ]

        # Add donor dashboard button if user is a registered donor
        if donor:
            keyboard.insert(0, [InlineKeyboardButton("📊 My Donor Dashboard", callback_data='open_donor_dashboard')])

        # Add admin button if the user is an admin
        if update.effective_user.id == int(os.getenv('ADMIN_ID', '0')):
            keyboard.append([InlineKeyboardButton("👑 Admin Dashboard", callback_data='open_admin_dashboard')])

        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            f'Hello {user.first_name}! Welcome to the Blood Donation Bot.\n\n'
            'This bot helps connect blood donors with people who need blood donations.',
            reply_markup=reply_markup
        )

    except Exception as e:
        logger.error(f"Error showing main menu: {e}")

        # Try to show a basic menu as fallback
        try:
            keyboard = [
                [InlineKeyboardButton("Request Blood", callback_data='request_blood')],
                [InlineKeyboardButton("View Donors", callback_data='view_donors')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                f'Hello! Welcome to the Blood Donation Bot.\n\n'
                'This bot helps connect blood donors with people who need blood donations.',
                reply_markup=reply_markup
            )
        except Exception:
            pass


async def admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin dashboard command - ADMIN ONLY."""
    try:
        user_id = update.effective_user.id
        admin_id = int(os.getenv('ADMIN_ID', '0'))

        # Check if the user is admin
        if user_id != admin_id:
            await update.message.reply_text("⛔ This command is restricted to administrators only.")
            return

        # Show admin dashboard message with buttons
        await admin_dashboard_message(update, context)

    except Exception as e:
        logger.error(f"Error in admin dashboard: {e}")
        await update.message.reply_text(f"⚠️ Error: {str(e)}")


async def admin_operation_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Command to list all successful donation operations."""
    try:
        user_id = update.effective_user.id
        admin_id = int(os.getenv('ADMIN_ID', '0'))

        # Check if the user is admin
        if user_id != admin_id:
            await update.message.reply_text("⛔ This command is restricted to administrators only.")
            return

        # Get successful operations from database
        try:
            successful_operations = db.get_recent_operations(15)  # Get up to 15 recent operations
        except Exception as e:
            logger.error(f"Failed to get recent operations: {e}")
            successful_operations = []

        if not successful_operations:
            # Create keyboard with buttons
            keyboard = [
                [InlineKeyboardButton("📊 View Dashboard", callback_data='admin_back_to_dashboard')],
                [InlineKeyboardButton("📱 Main Menu", callback_data='show_main_menu')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                "No successful donation operations recorded yet.",
                reply_markup=reply_markup
            )
            return

        # Create operations list message
        operations_msg = "👑 *SUCCESSFUL DONATION OPERATIONS*\n\n"

        # Get total operations count
        try:
            stats = db.get_operations_stats()
            total_ops = stats.get('total_operations', len(successful_operations))
        except:
            total_ops = len(successful_operations)

        operations_msg += f"Total operations: {total_ops}\n\n"

        # Display operations
        for i, op in enumerate(successful_operations, 1):
            try:
                req = op.get('request', {})
                donor = op.get('donor', {})

                operations_msg += (
                    f"*OPERATION #{i}*\n"
                    f"*Request ID:* `{req.get('id', 'N/A')}` | *Donor ID:* `{donor.get('id', 'N/A')}`\n"
                    f"*Patient:* {req.get('name', 'N/A')} | *Blood:* {req.get('blood_group', 'N/A')}\n"
                    f"*Donor:* {donor.get('name', 'N/A')} | *Blood:* {donor.get('blood_group', 'N/A')}\n"
                    f"*Hospital:* {req.get('hospital_name', 'N/A')}\n"
                    f"*Date:* {op.get('operation_date', op.get('date', 'N/A'))}\n"
                    f"-------------------------\n"
                )
            except Exception as e:
                logger.error(f"Error formatting operation {i}: {e}")
                continue

        # Create keyboard with buttons
        keyboard = [
            [InlineKeyboardButton("📊 View Dashboard", callback_data='admin_back_to_dashboard')],
            [InlineKeyboardButton("📱 Main Menu", callback_data='show_main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # If the message is too long, split it
        if len(operations_msg) > 4000:
            # Send the first part
            await update.message.reply_text(
                operations_msg[:4000],
                parse_mode='Markdown'
            )

            # Send the second part with buttons
            await update.message.reply_text(
                operations_msg[4000:] + "\n\nUse the buttons below to navigate:",
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            # Send as a single message
            await update.message.reply_text(
                operations_msg,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )

    except Exception as e:
        logger.error(f"Error in admin operations command: {e}")
        await update.message.reply_text(f"⚠️ Error: {str(e)}")


# Helper function to count donors by blood type
def count_donors_by_blood_type():
    """Count donors by blood type."""
    try:
        donors = db.get_all_donors()
        blood_counts = {'A+': 0, 'A-': 0, 'B+': 0, 'B-': 0, 'AB+': 0, 'AB-': 0, 'O+': 0, 'O-': 0}

        for donor in donors:
            blood_group = donor['blood_group']
            if blood_group in blood_counts:
                blood_counts[blood_group] += 1

        return blood_counts
    except Exception as e:
        logger.error(f"Error counting donors by blood type: {e}")
        return {'A+': 0, 'A-': 0, 'B+': 0, 'B-': 0, 'AB+': 0, 'AB-': 0, 'O+': 0, 'O-': 0}

logger.info("Starting admin_stats_command")
async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Command to show system statistics."""
    try:
        # Determine if this is from a callback query or direct command
        is_callback = hasattr(update, 'callback_query')

        if is_callback:
            query = update.callback_query
            user_id = query.from_user.id
        else:
            user_id = update.effective_user.id

        admin_id = int(os.getenv('ADMIN_ID', '0'))

        # Check if the user is admin
        if user_id != admin_id:
            if is_callback:
                await query.edit_message_text("⛔ This command is restricted to administrators only.")
            else:
                await update.message.reply_text("⛔ This command is restricted to administrators only.")
            return

        # Get statistics from database
        stats = db.get_operations_stats()

        # Calculate fulfillment rates
        fulfillment_rate = 0
        if stats['total_requests'] > 0:
            fulfillment_rate = (stats['total_operations'] / stats['total_requests']) * 100

        # Create message
        stats_msg = (
            "📊 *SYSTEM STATISTICS*\n\n"

            "*GENERAL STATISTICS:*\n"
            f"• Total registered donors: {stats['total_donors']}\n"
            f"• Total blood requests: {stats['total_requests']}\n"
            f"• Active blood requests: {stats['active_requests']}\n"
            f"• Successful donations: {stats['total_operations']}\n"
            f"• Request fulfillment rate: {fulfillment_rate:.1f}%\n\n"
        )

        # Add blood group statistics
        stats_msg += "*DONOR BLOOD GROUPS:*\n"
        blood_counts = count_donors_by_blood_type()
        for blood_type, count in blood_counts.items():
            if count > 0:
                stats_msg += f"• {blood_type}: {count} donors\n"

        # Add buttons
        keyboard = [
            [InlineKeyboardButton("📊 View Dashboard", callback_data='admin_back_to_dashboard')],
            [InlineKeyboardButton("📱 Main Menu", callback_data='show_main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Send or edit message based on context
        if is_callback:
            await query.edit_message_text(
                stats_msg,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                stats_msg,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )

    except Exception as e:
        logger.error(f"Error in admin stats command: {e}")
        if hasattr(update, 'callback_query'):
            await update.callback_query.edit_message_text(f"⚠️ Error: {str(e)}")
        else:
            await update.message.reply_text(f"⚠️ Error: {str(e)}")
            logger.info(f"Retrieved stats: {stats}")


async def admin_dashboard_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display the admin dashboard with statistics and action buttons."""
    try:
        # Get statistics from the database module
        stats = db.get_operations_stats()
        total_donors = stats.get('total_donors', 0)
        total_requests = stats.get('total_requests', 0)
        active_requests = stats.get('active_requests', 0)
        successful_operations = stats.get('total_operations', 0)

        # Create keyboard with admin actions - PROPERLY STRUCTURED
        keyboard = [
            [InlineKeyboardButton("📊 View Statistics", callback_data='admin_stats'),
             InlineKeyboardButton("🩸 View Donors", callback_data='admin_view_donors')],
            [InlineKeyboardButton("🏥 View Requests", callback_data='admin_view_requests'),
             InlineKeyboardButton("✅ Successful Operations", callback_data='admin_view_operations')],
            [InlineKeyboardButton("🔧 Manage Requests", callback_data='admin_manage_requests'),
             InlineKeyboardButton("👤 Manage Users", callback_data='admin_manage_users')],
            [InlineKeyboardButton("📨 Messaging Center", callback_data='admin_messaging_menu')],  # ✅ Ensure this line exists
            [InlineKeyboardButton("📬 Support Messages", callback_data='admin_view_support')],
            [InlineKeyboardButton("⚙️ System Settings", callback_data='admin_settings'),
             InlineKeyboardButton("📱 Main Menu", callback_data='show_main_menu')]
        ]

        # Make sure each row is a list of InlineKeyboardButtons
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Create message
        message = (
            "👑 *ADMIN DASHBOARD*\n\n"
            f"*SYSTEM OVERVIEW:*\n"
            f"• Total donors: {total_donors}\n"
            f"• Total requests: {total_requests}\n"
            f"• Active requests: {active_requests}\n"
            f"• Successful donations: {successful_operations}\n\n"
        )

        # Add blood type statistics
        message += f"*ACTIVE REQUESTS BY BLOOD TYPE:*\n"
        blood_counts = count_donors_by_blood_type()
        for blood_type, count in blood_counts.items():
            if count > 0:
                message += f"• {blood_type}: {count} donors\n"

        message += "\nSelect an option below to manage the blood donation system:"

        # If this was called from a callback_query, edit the message
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        # Otherwise, send a new message
        else:
            await update.message.reply_text(
                message,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )

    except Exception as e:
        logger.error(f"Error in admin dashboard message: {e}")

        # Determine which method to use based on the update
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.message.reply_text(f"⚠️ Error: {str(e)}")
        else:
            await update.message.reply_text(f"⚠️ Error: {str(e)}")


async def admin_deactivate_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mark a request as inactive."""
    query = update.callback_query
    await query.answer()

    # Extract request ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid request ID.")
        return

    request_id = parts[3]

    # Ask for confirmation
    keyboard = [
        [InlineKeyboardButton("Yes, Deactivate", callback_data=f"admin_confirm_deactivate_{request_id}")],
        [InlineKeyboardButton("No, Cancel", callback_data=f"admin_edit_request_{request_id}")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"Are you sure you want to mark request #{request_id} as inactive? "
        f"This will hide it from the active requests list.",
        reply_markup=reply_markup
    )


async def admin_manage_requests(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display interface for managing blood requests."""
    query = update.callback_query
    await query.answer()

    # Get all active requests
    active_requests = db.get_active_requests()

    if not active_requests:
        # No active requests
        keyboard = [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "🔧 *REQUEST MANAGEMENT*\n\n"
            "There are no active blood requests to manage.\n\n"
            "Use the button below to return to the dashboard.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return

    # Show list of active requests with management options
    message = "🔧 *REQUEST MANAGEMENT*\n\n" \
              "Select a request to manage:\n\n"

    # Create keyboard with options for each request
    keyboard = []
    for req in active_requests[:10]:  # Limit to 10 requests to avoid button overflow
        # Create a short summary for each request
        req_summary = f"{req['name']} - {req['blood_group']} - {req['urgency']}"

        # Add a button for each request
        keyboard.append([InlineKeyboardButton(
            req_summary,
            callback_data=f"admin_edit_request_{req['id']}"
        )])

    # Add back button
    keyboard.append([InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def admin_confirm_deactivate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirm marking a request as inactive."""
    query = update.callback_query
    await query.answer()

    # Extract request ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid request ID.")
        return

    request_id = parts[3]

    # Update database
    try:
        success = db.update_request_status(request_id, 'inactive')

        if success:
            await query.message.reply_text(f"Request #{request_id} marked as inactive.")
        else:
            await query.message.reply_text(f"Failed to update request #{request_id}.")

    except Exception as e:
        logger.error(f"Error updating request status: {e}")
        await query.message.reply_text(f"Error updating request: {str(e)}")

    # Go back to request management
    await admin_manage_requests(update, context)


async def admin_view_operations(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """View successful donation operations."""
    query = update.callback_query
    await query.answer()

    # Get recent operations (limit to 10 for now)
    operations = db.get_recent_operations(10)

    if not operations:
        keyboard = [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "✅ *SUCCESSFUL OPERATIONS*\n\n"
            "No successful donation operations recorded yet.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return

    # Create operations list
    message = "✅ *SUCCESSFUL OPERATIONS*\n\n"

    for i, op in enumerate(operations, 1):
        req = op['request']
        donor = op['donor']

        message += (
            f"*Operation #{i}*\n"
            f"*Date:* {op.get('operation_date', 'Unknown')}\n"
            f"*Donor:* {donor.get('name', 'Unknown')} ({donor.get('blood_group', 'Unknown')})\n"
            f"*Recipient:* {req.get('name', 'Unknown')} ({req.get('blood_group', 'Unknown')})\n"
            f"*Hospital:* {req.get('hospital_name', 'Unknown')}\n"
            f"---------------------\n"
        )

    # Add buttons
    keyboard = [
        [InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # If message is too long, truncate it
    if len(message) > 4000:
        message = message[:3900] + "\n\n... (more operations available)"

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_manage_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display interface for managing users (donors)."""
    query = update.callback_query
    await query.answer()

    # Get all donors
    all_donors = db.get_all_donors()

    if not all_donors:
        # No donors registered
        keyboard = [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "👤 *USER MANAGEMENT*\n\n"
            "There are no registered donors to manage.\n\n"
            "Use the button below to return to the dashboard.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return

    # Show list of donors with management options
    message = "👤 *USER MANAGEMENT*\n\n" \
              "Select a user to manage:\n\n"

    # Create keyboard with options for each donor
    keyboard = []
    for donor in all_donors[:10]:  # Limit to 10 donors to avoid button overflow
        # Create a short summary for each donor
        donor_summary = f"{donor['name']} - {donor['blood_group']} - {donor['district']}"

        # Add a button for each donor
        keyboard.append([InlineKeyboardButton(
            donor_summary,
            callback_data=f"admin_edit_user_{donor['id']}"
        )])

    # Add back button
    keyboard.append([InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_edit_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show options to edit a specific user."""
    query = update.callback_query
    await query.answer()

    # Extract user ID from callback data
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid user ID.")
        return

    donor_id = parts[3]

    # Get donor details
    donor = db.get_donor_by_id(donor_id)
    if not donor:
        await query.message.reply_text(f"User with ID {donor_id} not found.")
        return

    # Create message with donor details
    message = (
        f"👤 *EDITING USER #{donor_id}*\n\n"
        f"*Name:* {donor['name']}\n"
        f"*Blood Group:* {donor['blood_group']}\n"
        f"*Age:* {donor['age']}\n"
        f"*Gender:* {donor['gender']}\n"
        f"*Location:* {donor['area']}, {donor['district']}, {donor['division']}\n"
        f"*Contact:* {donor['phone']}\n"
        f"*Registration:* {donor['registration_date']}\n\n"
        f"Select an action:"
    )

    # Create keyboard with edit options
    keyboard = [
        [InlineKeyboardButton("Delete User", callback_data=f"admin_delete_user_{donor_id}")],
        [InlineKeyboardButton("Back to Users", callback_data="admin_manage_users")],
        [InlineKeyboardButton("Back to Dashboard", callback_data="admin_back_to_dashboard")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_delete_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delete a user."""
    query = update.callback_query
    await query.answer()

    # Extract donor ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid user ID.")
        return

    donor_id = parts[3]

    # Get donor details
    donor = db.get_donor_by_id(donor_id)
    if not donor:
        await query.message.reply_text(f"User with ID {donor_id} not found.")
        return

    # Ask for confirmation
    keyboard = [
        [InlineKeyboardButton("Yes, Delete User", callback_data=f"admin_confirm_delete_user_{donor_id}")],
        [InlineKeyboardButton("No, Cancel", callback_data=f"admin_edit_user_{donor_id}")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"⚠️ Are you sure you want to PERMANENTLY DELETE user {donor['name']} (ID: {donor_id})?\n\n"
        f"This will remove all user data and cannot be undone.",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_confirm_delete_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirm and delete a user."""
    query = update.callback_query
    await query.answer()

    # Extract donor ID
    parts = query.data.split('_')
    if len(parts) < 5:
        await query.message.reply_text("Invalid user ID.")
        return

    donor_id = parts[4]

    # Delete from database
    try:
        success = db.delete_donor(donor_id)

        if success:
            await query.message.reply_text(f"User with ID {donor_id} has been deleted.")
        else:
            await query.message.reply_text(f"Failed to delete user with ID {donor_id}.")

    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        await query.message.reply_text(f"Error deleting user: {str(e)}")

    # Go back to user management
    await admin_manage_users(update, context)


async def admin_view_donors(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """View all registered donors."""
    query = update.callback_query
    await query.answer()

    # Get donors (limit to 15 for now)
    donors = db.get_all_donors()[:15]

    if not donors:
        keyboard = [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "🩸 *REGISTERED DONORS*\n\n"
            "No donors registered yet.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return

    # Create donor list with more details for admin view
    message = "🩸 *REGISTERED DONORS*\n\n"

    for donor in donors:
        message += (
            f"*ID:* {donor['id']} | *Name:* {donor['name']}\n"
            f"*Blood:* {donor['blood_group']} | *Contact:* {donor['phone']}\n"
            f"*Location:* {donor['district']}, {donor['division']}\n"
            f"*Registered:* {donor['registration_date']}\n"
            f"---------------------\n"
        )

    # Add management buttons
    keyboard = [
        [InlineKeyboardButton("Manage Users", callback_data='admin_manage_users')],
        [InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # If message is too long, truncate it
    if len(message) > 4000:
        message = message[:3900] + "\n\n... (more donors available)"

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_view_requests(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """View all active blood requests."""
    query = update.callback_query
    await query.answer()

    # Get active requests
    active_requests = db.get_active_requests()

    if not active_requests:
        keyboard = [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "🏥 *ACTIVE BLOOD REQUESTS*\n\n"
            "No active blood requests at the moment.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return

    # Create request list with more details for admin view
    message = "🏥 *ACTIVE BLOOD REQUESTS*\n\n"

    for req in active_requests[:10]:  # Limit to 10 to avoid message too long
        message += (
            f"*ID:* {req['id']} | *Patient:* {req['name']}\n"
            f"*Blood:* {req['blood_group']} | *Urgency:* {req['urgency']}\n"
            f"*Hospital:* {req['hospital_name']}\n"
            f"*Contact:* {req['phone']} | *Date:* {req['request_date']}\n"
            f"---------------------\n"
        )

    # Add management buttons
    keyboard = [
        [InlineKeyboardButton("Manage Requests", callback_data='admin_manage_requests')],
        [InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # If message is too long, truncate it
    if len(message) > 4000:
        message = message[:3900] + "\n\n... (more requests available)"

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_unrestrict_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove restriction from a user."""
    query = update.callback_query
    await query.answer()

    # Extract donor ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid user ID.")
        return

    donor_id = parts[3]

    # Update database
    try:
        success = db.update_donor_restriction(donor_id, False)

        if success:
            await query.message.reply_text(f"Restriction removed from user with ID {donor_id}.")
        else:
            await query.message.reply_text(f"Failed to update user with ID {donor_id}.")

    except Exception as e:
        logger.error(f"Error updating user restriction: {e}")
        await query.message.reply_text(f"Error updating user: {str(e)}")

    # Go back to user editing
    await admin_edit_user(update, context)


async def admin_search_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt admin to search for users."""
    query = update.callback_query
    await query.answer()

    # Store that we're in search mode
    context.user_data['admin_searching_users'] = True

    # Prompt for search
    keyboard = [[InlineKeyboardButton("Cancel Search", callback_data='admin_manage_users')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        "🔍 *USER SEARCH*\n\n"
        "Please enter a search term (name, blood group, phone number, or location)\n\n"
        "Type your search term and send it as a message.",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    # This will set up to receive the next message as a search term
    # You'll need to add a message handler for this


async def admin_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show system settings for the admin."""
    query = update.callback_query
    await query.answer()

    # Get current settings - you'll need to implement a system for settings
    # This is a placeholder

    message = (
        "⚙️ *SYSTEM SETTINGS*\n\n"
        "*Notification Settings:*\n"
        "• Auto-notify donors: Enabled\n"
        "• Notification radius: 50 km\n\n"
        "*System Configuration:*\n"
        "• Database size: 2.4 MB\n"
        "• Registered users: 24\n"
        "• Active requests: 5\n\n"
        "Select an option to modify:"
    )

    keyboard = [
        [InlineKeyboardButton("Notification Settings", callback_data='admin_notification_settings')],
        [InlineKeyboardButton("System Maintenance", callback_data='admin_system_maintenance')],
        [InlineKeyboardButton("Database Backup", callback_data='admin_database_backup')],
        [InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_edit_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show options to edit a specific blood request."""
    query = update.callback_query
    await query.answer()

    # Extract request ID from callback data
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid request ID.")
        return

    request_id = parts[3]

    # Get request details
    request = db.get_request_by_id(request_id)
    if not request:
        await query.message.reply_text(f"Request with ID {request_id} not found.")
        return

    # Create message with request details
    message = (
        f"🔧 *EDITING REQUEST #{request_id}*\n\n"
        f"*Patient:* {request['name']}\n"
        f"*Age:* {request['age']}\n"
        f"*Blood Group:* {request['blood_group']}\n"
        f"*Hospital:* {request['hospital_name']}\n"
        f"*Location:* {request['area']}, {request['district']}, {request['division']}\n"
        f"*Current Urgency:* {request['urgency']}\n"
        f"*Status:* {request['status']}\n"
        f"*Contact:* {request['phone']}\n\n"
        f"Select an action:"
    )

    # Create keyboard with edit options
    keyboard = [
        [InlineKeyboardButton("Change Urgency", callback_data=f"admin_change_urgency_{request_id}")],
        [InlineKeyboardButton("Mark as Fulfilled", callback_data=f"admin_fulfill_request_{request_id}")],
        [InlineKeyboardButton("Mark as Inactive", callback_data=f"admin_deactivate_request_{request_id}")],
        [InlineKeyboardButton("Delete Request", callback_data=f"admin_delete_request_{request_id}")],
        [InlineKeyboardButton("Back to Requests", callback_data="admin_manage_requests")],
        [InlineKeyboardButton("Back to Dashboard", callback_data="admin_back_to_dashboard")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_change_urgency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Change the urgency level of a request."""
    query = update.callback_query
    await query.answer()

    # Extract request ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid request ID.")
        return

    request_id = parts[3]

    # Create urgency selection keyboard
    keyboard = [
        [InlineKeyboardButton("Urgent", callback_data=f"admin_set_urgency_{request_id}_Urgent")],
        [InlineKeyboardButton("High", callback_data=f"admin_set_urgency_{request_id}_High")],
        [InlineKeyboardButton("Medium", callback_data=f"admin_set_urgency_{request_id}_Medium")],
        [InlineKeyboardButton("Low", callback_data=f"admin_set_urgency_{request_id}_Low")],
        [InlineKeyboardButton("Cancel", callback_data=f"admin_edit_request_{request_id}")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"Select new urgency level for request #{request_id}:",
        reply_markup=reply_markup
    )


async def admin_set_urgency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Set the urgency level for a request."""
    query = update.callback_query
    await query.answer()

    # Extract data from callback
    parts = query.data.split('_')
    if len(parts) < 5:
        await query.message.reply_text("Invalid data format.")
        return

    request_id = parts[3]
    urgency = parts[4]

    # Update database
    try:
        # Modify your database function to update urgency field
        success = db.update_request_field(request_id, 'urgency', urgency)

        if success:
            await query.message.reply_text(f"Urgency for request #{request_id} set to {urgency}.")
        else:
            await query.message.reply_text(f"Failed to update urgency for request #{request_id}.")

    except Exception as e:
        logger.error(f"Error updating request urgency: {e}")
        await query.message.reply_text(f"Error updating request: {str(e)}")

    # Return to request editing
    await admin_edit_request(update, context)


async def admin_fulfill_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mark a request as fulfilled."""
    query = update.callback_query
    await query.answer()

    # Extract request ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid request ID.")
        return

    request_id = parts[3]

    # Ask for confirmation
    keyboard = [
        [InlineKeyboardButton("Yes, Mark as Fulfilled", callback_data=f"admin_confirm_fulfill_{request_id}")],
        [InlineKeyboardButton("No, Cancel", callback_data=f"admin_edit_request_{request_id}")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"Are you sure you want to mark request #{request_id} as fulfilled?",
        reply_markup=reply_markup
    )


async def admin_confirm_fulfill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirm marking a request as fulfilled."""
    query = update.callback_query
    await query.answer()

    # Extract request ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid request ID.")
        return

    request_id = parts[3]

    # Update database
    try:
        success = db.update_request_status(request_id, 'fulfilled')

        if success:
            await query.message.reply_text(f"Request #{request_id} marked as fulfilled.")
        else:
            await query.message.reply_text(f"Failed to update request #{request_id}.")

    except Exception as e:
        logger.error(f"Error updating request status: {e}")
        await query.message.reply_text(f"Error updating request: {str(e)}")

    # Go back to request management
    await admin_manage_requests(update, context)


async def admin_delete_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delete a request."""
    query = update.callback_query
    await query.answer()

    # Extract request ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid request ID.")
        return

    request_id = parts[3]

    # Ask for confirmation
    keyboard = [
        [InlineKeyboardButton("Yes, Delete Request", callback_data=f"admin_confirm_delete_request_{request_id}")],
        [InlineKeyboardButton("No, Cancel", callback_data=f"admin_edit_request_{request_id}")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"⚠️ Are you sure you want to PERMANENTLY DELETE request #{request_id}?\n\n"
        f"This action cannot be undone.",
        reply_markup=reply_markup
    )


async def admin_confirm_delete_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirm and delete a request."""
    query = update.callback_query
    await query.answer()

    # Extract request ID
    parts = query.data.split('_')
    if len(parts) < 5:
        await query.message.reply_text("Invalid request ID.")
        return

    request_id = parts[4]

    # Delete from database
    try:
        success = db.delete_request(request_id)

        if success:
            await query.message.reply_text(f"Request #{request_id} has been deleted.")
        else:
            await query.message.reply_text(f"Failed to delete request #{request_id}.")

    except Exception as e:
        logger.error(f"Error deleting request: {e}")
        await query.message.reply_text(f"Error deleting request: {str(e)}")

    # Go back to request management
    await admin_manage_requests(update, context)


async def admin_manage_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display interface for managing users (donors)."""
    query = update.callback_query
    await query.answer()

    # Get all donors
    all_donors = db.get_all_donors()

    if not all_donors:
        # No donors registered
        keyboard = [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "👤 *USER MANAGEMENT*\n\n"
            "There are no registered donors to manage.\n\n"
            "Use the button below to return to the dashboard.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return

    # Show list of donors with management options
    message = "👤 *USER MANAGEMENT*\n\n" \
              "Select a user to manage:\n\n"

    # Create keyboard with options for each donor
    keyboard = []
    for donor in all_donors[:10]:  # Limit to 10 donors to avoid button overflow
        # Create a short summary for each donor
        donor_summary = f"{donor['name']} - {donor['blood_group']} - {donor['district']}"

        # Add a button for each donor
        keyboard.append([InlineKeyboardButton(
            donor_summary,
            callback_data=f"admin_edit_user_{donor['id']}"
        )])

    # Add user search option
    keyboard.append([InlineKeyboardButton("🔍 Search Users", callback_data='admin_search_users')])

    # Add back button
    keyboard.append([InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_edit_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show options to edit a specific user."""
    query = update.callback_query
    await query.answer()

    # Extract user ID from callback data
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid user ID.")
        return

    donor_id = parts[3]

    # Get donor details
    donor = db.get_donor_by_id(donor_id)
    if not donor:
        await query.message.reply_text(f"User with ID {donor_id} not found.")
        return

    # Create message with donor details
    message = (
        f"👤 *EDITING USER #{donor_id}*\n\n"
        f"*Name:* {donor['name']}\n"
        f"*Blood Group:* {donor['blood_group']}\n"
        f"*Age:* {donor['age']}\n"
        f"*Gender:* {donor['gender']}\n"
        f"*Location:* {donor['area']}, {donor['district']}, {donor['division']}\n"
        f"*Contact:* {donor['phone']}\n"
        f"*Registration:* {donor['registration_date']}\n\n"
        f"Select an action:"
    )

    # Create keyboard with edit options
    keyboard = [
        [InlineKeyboardButton("View Donation History", callback_data=f"admin_user_history_{donor_id}")],
        [InlineKeyboardButton("Restrict User", callback_data=f"admin_restrict_user_{donor_id}")],
        [InlineKeyboardButton("Delete User", callback_data=f"admin_delete_user_{donor_id}")],
        [InlineKeyboardButton("Back to Users", callback_data="admin_manage_users")],
        [InlineKeyboardButton("Back to Dashboard", callback_data="admin_back_to_dashboard")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_user_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show donation history for a user."""
    query = update.callback_query
    await query.answer()

    # Extract donor ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid user ID.")
        return

    donor_id = parts[3]

    # Get donor details and stats
    donor = db.get_donor_by_id(donor_id)
    if not donor:
        await query.message.reply_text(f"User with ID {donor_id} not found.")
        return

    # Get donor stats and donation history
    donor_stats = db.get_donor_stats(donor_id)

    # Create message
    message = (
        f"📋 *DONATION HISTORY - {donor['name']}*\n\n"
        f"*Total Donations:* {donor_stats['total_donations']}\n"
        f"*Fulfilled Donations:* {donor_stats['fulfilled_donations']}\n"
        f"*Pending Donations:* {donor_stats['pending_donations']}\n\n"
    )

    # Add donation history if available
    # You'll need to implement a function to get detailed donation history

    # Add back button
    keyboard = [[InlineKeyboardButton("Back to User", callback_data=f"admin_edit_user_{donor_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_restrict_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Restrict a user from using the bot."""
    query = update.callback_query
    await query.answer()

    # Extract donor ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid user ID.")
        return

    donor_id = parts[3]

    # Get donor details
    donor = db.get_donor_by_id(donor_id)
    if not donor:
        await query.message.reply_text(f"User with ID {donor_id} not found.")
        return

    # Check if already restricted
    is_restricted = donor.get('is_restricted', False)  # You'll need to add this field to your schema

    # Create message and keyboard based on current status
    if is_restricted:
        message = f"User {donor['name']} is currently restricted. Do you want to remove this restriction?"
        keyboard = [
            [InlineKeyboardButton("Remove Restriction", callback_data=f"admin_unrestrict_user_{donor_id}")],
            [InlineKeyboardButton("Cancel", callback_data=f"admin_edit_user_{donor_id}")]
        ]
    else:
        message = (
            f"Are you sure you want to restrict user {donor['name']}?\n\n"
            f"Restricted users cannot:\n"
            f"• Make new donation requests\n"
            f"• Accept donation requests\n"
            f"• Interact with the bot in any way\n\n"
            f"This action can be reversed later."
        )
        keyboard = [
            [InlineKeyboardButton("Yes, Restrict User", callback_data=f"admin_confirm_restrict_{donor_id}")],
            [InlineKeyboardButton("No, Cancel", callback_data=f"admin_edit_user_{donor_id}")]
        ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        reply_markup=reply_markup
    )


async def admin_confirm_restrict(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirm and restrict a user."""
    query = update.callback_query
    await query.answer()

    # Extract donor ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid user ID.")
        return

    donor_id = parts[3]

    # Update database to restrict user
    try:
        # You'll need to implement this function
        success = db.update_donor_restriction(donor_id, True)

        if success:
            await query.message.reply_text(f"User with ID {donor_id} has been restricted.")
        else:
            await query.message.reply_text(f"Failed to restrict user with ID {donor_id}.")

    except Exception as e:
        logger.error(f"Error restricting user: {e}")
        await query.message.reply_text(f"Error restricting user: {str(e)}")

    # Go back to user editing
    await admin_edit_user(update, context)


async def admin_delete_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delete a user."""
    query = update.callback_query
    await query.answer()

    # Extract donor ID
    parts = query.data.split('_')
    if len(parts) < 4:
        await query.message.reply_text("Invalid user ID.")
        return

    donor_id = parts[3]

    # Get donor details
    donor = db.get_donor_by_id(donor_id)
    if not donor:
        await query.message.reply_text(f"User with ID {donor_id} not found.")
        return

    # Ask for confirmation
    keyboard = [
        [InlineKeyboardButton("Yes, Delete User", callback_data=f"admin_confirm_delete_user_{donor_id}")],
        [InlineKeyboardButton("No, Cancel", callback_data=f"admin_edit_user_{donor_id}")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"⚠️ Are you sure you want to PERMANENTLY DELETE user {donor['name']} (ID: {donor_id})?\n\n"
        f"This will remove all user data and cannot be undone.",
        reply_markup=reply_markup
    )


async def admin_confirm_delete_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Confirm and delete a user."""
    query = update.callback_query
    await query.answer()

    # Extract donor ID
    parts = query.data.split('_')
    if len(parts) < 5:
        await query.message.reply_text("Invalid user ID.")
        return

    donor_id = parts[4]

    # Delete from database
    try:
        success = db.delete_donor(donor_id)

        if success:
            await query.message.reply_text(f"User with ID {donor_id} has been deleted.")
        else:
            await query.message.reply_text(f"Failed to delete user with ID {donor_id}.")

    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        await query.message.reply_text(f"Error deleting user: {str(e)}")

    # Go back to user management
    await admin_manage_users(update, context)


async def admin_search_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle admin search input."""
    # Check if this is actually a search input from an admin
    if not context.user_data.get('admin_searching_users', False):
        return

    # Check if user is admin
    admin_id = int(os.getenv('ADMIN_ID', '0'))
    if update.effective_user.id != admin_id:
        return

    # Clear the search flag
    context.user_data['admin_searching_users'] = False

    # Get the search term
    search_term = update.message.text.strip()

    # Search for donors
    matching_donors = db.search_donors(search_term)

    if not matching_donors:
        keyboard = [[InlineKeyboardButton("Back to User Management", callback_data='admin_manage_users')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            f"🔍 *SEARCH RESULTS*\n\n"
            f"No donors found matching '{search_term}'.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return

    # Display results
    message = f"🔍 *SEARCH RESULTS FOR '{search_term}'*\n\n"

    # Create keyboard with options for each matching donor
    keyboard = []
    for donor in matching_donors[:10]:  # Limit to 10 results
        # Create a short summary for each donor
        donor_summary = f"{donor['name']} - {donor['blood_group']} - {donor['district']}"

        # Add a button for each donor
        keyboard.append([InlineKeyboardButton(
            donor_summary,
            callback_data=f"admin_edit_user_{donor['id']}"
        )])

    # Add back button
    keyboard.append([InlineKeyboardButton("Back to User Management", callback_data='admin_manage_users')])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        message + f"Found {len(matching_donors)} matching donors. Select a user to manage:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def admin_system_maintenance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle system maintenance options."""
    query = update.callback_query
    await query.answer()

    message = (
        "⚙️ *SYSTEM MAINTENANCE*\n\n"
        "*Available Operations:*\n"
        "• Clear old requests (inactive for 30+ days)\n"
        "• Check database integrity\n"
        "• View system logs\n\n"
        "Select an operation:"
    )

    keyboard = [
        [InlineKeyboardButton("Clear Old Requests", callback_data='admin_clear_old_requests')],
        [InlineKeyboardButton("Check Database", callback_data='admin_check_database')],
        [InlineKeyboardButton("View Logs", callback_data='admin_view_logs')],
        [InlineKeyboardButton("Back to Settings", callback_data='admin_settings')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


# Add this function to check if a user is restricted
def is_user_restricted(telegram_id: int) -> bool:
    """Check if a user is restricted from using the bot."""
    try:
        conn = db.get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Check if user is restricted
        cursor.execute(
            'SELECT is_restricted FROM donors WHERE telegram_id = %s',
            (telegram_id,)
        )

        result = cursor.fetchone()
        conn.close()

        if result and result['is_restricted']:
            return True
        return False

    except Exception as e:
        logger.error(f"Error checking user restriction: {e}")
        return False


# Use this function in key action handlers, for example:
async def request_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start blood request process."""
    user_id = update.effective_user.id

    # Check if user is restricted
    if is_user_restricted(user_id):
        await update.message.reply_text(
            "⛔ *ACCESS RESTRICTED*\n\n"
            "You are currently restricted from making blood requests.\n"
            "Please contact an administrator if you believe this is an error.",
            parse_mode='Markdown'
        )
        return ConversationHandler.END

    await update.message.reply_text('Let\'s create a blood request. What is the patient\'s name?')
    return REQUEST_NAME


async def admin_database_backup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Explain PostgreSQL backup options."""
    message = "⚠️ *DATABASE BACKUP NOTICE*\n\n" + \
              "Your database is now PostgreSQL which cannot be backed up with a simple file copy.\n\n" + \
              "Please use standard PostgreSQL backup tools like:\n" + \
              "- `pg_dump` command line tool\n" + \
              "- Database management tools\n" + \
              "- Railway's built-in backup features\n\n" + \
              "Contact your database administrator for more details."

    try:
        if hasattr(update, 'callback_query'):
            query = update.callback_query
            await query.answer()

            keyboard = [[InlineKeyboardButton("Back to Settings", callback_data='admin_settings')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                message,
                parse_mode='Markdown'
            )
    except Exception as e:
        logger.error(f"Error displaying database backup information: {e}")

        # Handle error based on update type
        if hasattr(update, 'callback_query'):
            keyboard = [[InlineKeyboardButton("Back to Settings", callback_data='admin_settings')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.callback_query.edit_message_text(
                "❌ *DATABASE BACKUP ERROR*\n\n"
                f"Error: {str(e)}",
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "❌ *DATABASE BACKUP ERROR*\n\n"
                f"Error: {str(e)}",
                parse_mode='Markdown'
            )

async def handle_admin_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle callback queries for admin dashboard."""
    query = update.callback_query
    user_id = update.effective_user.id
    admin_id = int(os.getenv('ADMIN_ID', '0'))

    try:
        # Always acknowledge the callback query first
        await query.answer()

        # Check if user is admin
        if user_id != admin_id:
            await query.edit_message_text("⛔ This action is restricted to administrators only.")
            return

        callback_data = query.data

        if callback_data == 'admin_messaging_menu':
            await admin_messaging_menu(update, context)
            return

        # Handle different admin actions based on the callback data
        if callback_data == 'admin_stats':
            await admin_stats_command(update, context)
        elif callback_data == 'admin_view_donors':
            # Handle viewing donors
            donors = db.get_all_donors()

            if not donors:
                await query.edit_message_text("No donors registered yet.")
                return

            # Create a simple donor list for now
            donor_list = "👑 *ADMIN: ALL DONORS*\n\n"
            for donor in donors[:15]:  # Show first 15 to avoid message too long
                donor_list += (
                    f"*ID:* {donor['id']} | *Name:* {donor['name']}\n"
                    f"*Blood:* {donor['blood_group']} | *Phone:* {donor['phone']}\n"
                    f"*Location:* {donor['district']}, {donor['division']}\n"
                    f"---------------------\n"
                )

            # Add back button
            keyboard = [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(donor_list, reply_markup=reply_markup, parse_mode='Markdown')
        elif callback_data == 'admin_view_requests':
            # Handle viewing requests
            active_requests = db.get_active_requests()

            if not active_requests:
                await query.edit_message_text("No active requests at the moment.")
                return

            req_list = "👑 *ADMIN: ACTIVE REQUESTS*\n\n"
            for req in active_requests[:15]:  # Show first 15 to avoid message too long
                req_list += (
                    f"*ID:* {req['id']} | *Patient:* {req['name']}\n"
                    f"*Blood:* {req['blood_group']} | *Urgency:* {req['urgency']}\n"
                    f"*Hospital:* {req['hospital_name']}\n"
                    f"*Contact:* {req['phone']}\n"
                    f"---------------------\n"
                )

            # Add back button
            keyboard = [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(req_list, reply_markup=reply_markup, parse_mode='Markdown')
        elif callback_data == 'admin_view_operations':
            await admin_view_operations(update, context)
        elif callback_data == 'admin_manage_requests':
            await admin_manage_requests(update, context)
        elif callback_data == 'admin_manage_users':
            await admin_manage_users(update, context)
        elif callback_data.startswith('admin_edit_user_'):
            await admin_edit_user(update, context)
        elif callback_data.startswith('admin_delete_user_'):
            await admin_delete_user(update, context)
        elif callback_data.startswith('admin_confirm_delete_user_'):
            await admin_confirm_delete_user(update, context)
        elif callback_data.startswith('admin_user_history_'):
            await admin_user_history(update, context)
        elif callback_data.startswith('admin_restrict_user_'):
            await admin_restrict_user(update, context)
        elif callback_data.startswith('admin_confirm_restrict_'):
            await admin_confirm_restrict(update, context)
        elif callback_data.startswith('admin_unrestrict_user_'):
            await admin_unrestrict_user(update, context)
        elif callback_data == 'admin_search_users':
            await admin_search_users(update, context)
        elif callback_data.startswith('admin_edit_request_'):
            await admin_edit_request(update, context)
        elif callback_data.startswith('admin_deactivate_request_'):
            await admin_deactivate_request(update, context)
        elif callback_data.startswith('admin_confirm_deactivate_'):
            await admin_confirm_deactivate(update, context)
        elif callback_data.startswith('admin_change_urgency_'):
            await admin_change_urgency(update, context)
        elif callback_data.startswith('admin_set_urgency_'):
            await admin_set_urgency(update, context)
        elif callback_data.startswith('admin_fulfill_request_'):
            await admin_fulfill_request(update, context)
        elif callback_data.startswith('admin_confirm_fulfill_'):
            await admin_confirm_fulfill(update, context)
        elif callback_data.startswith('admin_delete_request_'):
            await admin_delete_request(update, context)
        elif callback_data.startswith('admin_confirm_delete_request_'):
            await admin_confirm_delete_request(update, context)
        elif callback_data == 'admin_settings':
            await admin_settings(update, context)
        elif callback_data == 'admin_system_maintenance':
            await admin_system_maintenance(update, context)
        elif callback_data == 'admin_database_backup':
            await admin_database_backup(update, context)
        elif callback_data == 'admin_back_to_dashboard':
            # Return to admin dashboard
            await admin_dashboard_message(update, context)
        else:
            # For any unimplemented admin action, use edit_message_text instead of reply_text
            await query.edit_message_text(f"Admin action '{callback_data}' is not implemented yet.")

    except Exception as e:
        logger.error(f"Error handling admin callback: {e}")
        try:
            # Always use edit_message_text for callbacks, not reply_text
            await query.edit_message_text(f"Error: {str(e)}")
        except Exception as inner_e:
            logger.error(f"Failed to edit message with error: {inner_e}")
            try:
                # If editing fails, try to send a new message
                await context.bot.send_message(chat_id=user_id, text=f"Error processing admin action: {str(e)}")
            except Exception as send_error:
                logger.error(f"Failed to send error message: {send_error}")

async def support_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the support/suggestion process."""
    await update.message.reply_text(
        "📬 *SUGGESTION & SUPPORT BOX*\n\n"
        "Please describe your suggestion, issue, or feedback in detail. "
        "This message will be sent directly to the administrators.\n\n"
        "Type your message now or use /cancel to abort.",
        parse_mode='Markdown'
    )
    return SUPPORT_MESSAGE


async def support_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store the support message and ask for confirmation."""
    # Explicitly log entry to this function with full details
    logger.info(
        f"ENTERED support_message function - User ID: {update.effective_user.id}, Message: {update.message.text}")

    try:
        user_message = update.message.text
        context.user_data['support_message'] = user_message

        # Get user information for context
        user = update.effective_user
        context.user_data['support_user'] = {
            'id': user.id,
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name
        }

        # Log user data has been stored
        logger.info(f"Support message stored in user_data for user {user.id}")

        # Create confirmation keyboard
        keyboard = [
            [InlineKeyboardButton("✅ Yes, Send it", callback_data='confirm_support')],
            [InlineKeyboardButton("❌ No, Cancel", callback_data='cancel_support')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "📝 *PREVIEW OF YOUR MESSAGE:*\n\n"
            f"{user_message}\n\n"
            "Are you sure you want to send this message to the administrators?",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

        # Log successful message send and state transition
        logger.info(
            f"Support preview sent to user {user.id}, transitioning to CONFIRM_SEND_SUPPORT ({CONFIRM_SEND_SUPPORT})")

        return CONFIRM_SEND_SUPPORT
    except Exception as e:
        logger.error(f"ERROR in support_message: {e}")
        await update.message.reply_text("Sorry, there was an error processing your message. Please try again.")
        return ConversationHandler.END


async def support_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle the confirmation of support message."""
    query = update.callback_query
    await query.answer()

    # Get admin ID from env
    admin_id = os.getenv('ADMIN_ID', '0')

    if query.data == 'confirm_support':
        try:
            # Get stored message and user info
            support_message = context.user_data.get('support_message', 'No message provided')
            user_info = context.user_data.get('support_user', {})

            # Format admin notification
            admin_notification = (
                "📬 *NEW SUGGESTION/SUPPORT REQUEST*\n\n"
                f"*From User:* {user_info.get('first_name', '')} {user_info.get('last_name', '')}\n"
                f"*Username:* @{user_info.get('username', 'N/A')}\n"
                f"*User ID:* `{user_info.get('id', 'N/A')}`\n\n"
                f"*Message:*\n{support_message}\n\n"
                f"*Timestamp:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                "To respond, you can use the user's Telegram ID."
            )

            # Send to admin
            await context.bot.send_message(
                chat_id=admin_id,
                text=admin_notification,
                parse_mode='Markdown'
            )

            # Notify the user
            await query.edit_message_text(
                "✅ *THANK YOU!*\n\n"
                "Your message has been sent to the administrators. "
                "They will review it and may contact you if necessary.\n\n"
                "Thank you for helping improve our blood donation system!",
                parse_mode='Markdown'
            )

            # Store in database if needed
            store_support_message(user_info, support_message)

        except Exception as e:
            logger.error(f"Error sending support message: {e}")
            await query.edit_message_text(
                "❌ *ERROR*\n\n"
                "There was an error sending your message. Please try again later.",
                parse_mode='Markdown'
            )
    else:
        # User canceled
        await query.edit_message_text(
            "🚫 *CANCELED*\n\n"
            "Your support message has been canceled and was not sent.",
            parse_mode='Markdown'
        )

    # Clear user data
    context.user_data.pop('support_message', None)
    context.user_data.pop('support_user', None)

    return ConversationHandler.END


async def admin_view_support_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """View all support messages - ADMIN ONLY."""
    try:
        user_id = update.effective_user.id
        admin_id = int(os.getenv('ADMIN_ID', '0'))

        # Check if the user is admin
        if user_id != admin_id:
            if hasattr(update, 'callback_query'):
                await update.callback_query.edit_message_text("⛔ This command is restricted to administrators only.")
            else:
                await update.message.reply_text("⛔ This command is restricted to administrators only.")
            return

        # Get support messages from database
        support_messages = get_support_messages()

        if not support_messages:
            message = "📬 *SUPPORT MESSAGES*\n\nNo support messages found."

            # Create keyboard
            keyboard = [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            if hasattr(update, 'callback_query'):
                await update.callback_query.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
            else:
                await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
            return

        # Create the message with support messages
        message = "📬 *SUPPORT MESSAGES*\n\n"

        for msg in support_messages[:10]:  # Show only 10 most recent
            message += (
                f"*ID:* {msg['id']} | *From:* {msg['user_name']}\n"
                f"*Date:* {msg['created_at']}\n"
                f"*Status:* {msg['status']}\n"
                f"*Message:* {msg['message'][:100]}{'...' if len(msg['message']) > 100 else ''}\n"
                f"---------------------\n"
            )

        # Add note if there are more messages
        if len(support_messages) > 10:
            message += f"\n*Note:* Showing 10 of {len(support_messages)} messages."

        # Create keyboard with actions
        keyboard = [
            [InlineKeyboardButton("Mark All as Read", callback_data='admin_mark_support_read')],
            [InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if hasattr(update, 'callback_query'):
            await update.callback_query.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error viewing support messages: {e}")
        error_message = f"Error viewing support messages: {str(e)}"

        if hasattr(update, 'callback_query'):
            await update.callback_query.edit_message_text(error_message)
        else:
            await update.message.reply_text(error_message)


async def admin_mark_support_read(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mark all support messages as read."""
    query = update.callback_query
    await query.answer()

    try:
        # Update database with PostgreSQL
        conn = db.get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
        UPDATE support_messages
        SET status = %s
        WHERE status = %s
        ''', ('read', 'pending'))

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        if rows_affected > 0:
            await query.edit_message_text(
                f"✅ Successfully marked {rows_affected} messages as read.\n\n"
                "Use the button below to return to the dashboard.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]),
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text(
                "No pending messages to mark as read.\n\n"
                "Use the button below to return to the dashboard.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]),
                parse_mode='Markdown'
            )
    except Exception as e:
        logger.error(f"Error marking support messages as read: {e}")
        await query.edit_message_text(
            f"Error marking messages as read: {str(e)}\n\n"
            "Use the button below to return to the dashboard.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]]),
            parse_mode='Markdown'
        )


async def admin_reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Direct reply to a user's support message - for admins only."""
    try:
        # Check if the user is admin
        user_id = update.effective_user.id
        admin_id = int(os.getenv('ADMIN_ID', '0'))

        if user_id != admin_id:
            await update.message.reply_text("⛔ This command is restricted to administrators only.")
            return

        # Extract user ID and message from command arguments
        args = context.args
        if not args or len(args) < 2:
            await update.message.reply_text(
                "Please provide a user ID and message.\n\n"
                "Usage: /reply [user_id] [message]"
            )
            return

        try:
            target_user_id = int(args[0])
            admin_reply = ' '.join(args[1:])

            # Format the message
            formatted_reply = (
                "📬 *REPLY FROM ADMINISTRATOR*\n\n"
                f"{admin_reply}\n\n"
                "If you need further assistance, feel free to send another message using /support."
            )

            # Send the message
            await context.bot.send_message(
                chat_id=target_user_id,
                text=formatted_reply,
                parse_mode='Markdown'
            )

            # Confirm to admin
            await update.message.reply_text(
                f"✅ Your reply has been sent to user ID: `{target_user_id}`\n\n"
                "If you need to send another reply, use /reply [user_id] [message].",
                parse_mode='Markdown'
            )

            # Record the reply in the database
            record_admin_reply(target_user_id, admin_reply)

        except ValueError:
            await update.message.reply_text("Invalid user ID. Please provide a valid numeric ID.")
        except Exception as e:
            logger.error(f"Error sending admin reply: {e}")
            await update.message.reply_text(f"Error sending reply: {str(e)}")

    except Exception as e:
        logger.error(f"Error in admin_reply_to_user: {e}")
        await update.message.reply_text(f"Error: {str(e)}")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel the current conversation."""
    user = update.effective_user
    logger.info(f"CANCEL FUNCTION CALLED: User {user.id} canceled the conversation.")

    await update.message.reply_text(
        "Operation canceled. What would you like to do now?",
        reply_markup=ReplyKeyboardRemove()
    )

    # Clear user data
    context.user_data.clear()
    logger.info("User data cleared, returning ConversationHandler.END")

    return ConversationHandler.END


async def admin_reply_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Process the admin's reply message."""
    admin_reply = update.message.text
    target_user_id = context.user_data.get('reply_to_user_id')

    if not target_user_id:
        await update.message.reply_text("Error: No target user specified. Please try again with /reply [user_id].")
        return ConversationHandler.END

    return await send_admin_reply(update, context, target_user_id, admin_reply)


async def send_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int,
                           reply_message: str) -> int:
    """Send the admin's reply to the user."""
    try:
        # Format the message
        formatted_reply = (
            "📬 *REPLY FROM ADMINISTRATOR*\n\n"
            f"{reply_message}\n\n"
            "If you need further assistance, feel free to send another message using /support."
        )

        # Send the message
        await context.bot.send_message(
            chat_id=target_user_id,
            text=formatted_reply,
            parse_mode='Markdown'
        )

        # Confirm to admin
        await update.message.reply_text(
            f"✅ Your reply has been sent to user ID: `{target_user_id}`\n\n"
            "If you need to send another reply, use /reply [user_id] [message].",
            parse_mode='Markdown'
        )

        # Record the reply in the database if needed
        record_admin_reply(target_user_id, reply_message)

        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Error sending admin reply: {e}")
        await update.message.reply_text(f"Error sending reply: {str(e)}")
        return ConversationHandler.END


async def open_support(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Open the support dialog from a button press."""
    logger.info("open_support function called with update: %s", update)
    query = update.callback_query
    await query.answer()

    logger.info("Sending support message prompt")
    await query.message.reply_text(
        "📬 *SUGGESTION & SUPPORT BOX*\n\n"
        "Please describe your suggestion, issue, or feedback in detail. "
        "This message will be sent directly to the administrators.\n\n"
        "Type your message now or use /cancel to abort.",
        parse_mode='Markdown'
    )
    logger.info("Returning SUPPORT_MESSAGE state: %s", SUPPORT_MESSAGE)
    return SUPPORT_MESSAGE
async def debug_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Debug handler to log all incoming messages"""
    logger.info(f"Debug: Received message: {update.message.text}")
    # Don't handle the message, just log it
    return None
async def unhandled_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log unhandled messages for debugging."""
    if update.message and update.message.text:
        logger.warning(f"UNHANDLED MESSAGE: {update.message.text}")
        # Don't reply to avoid confusion


async def admin_messaging_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show messaging options for admins."""
    try:
        # Check if user is admin
        user_id = update.effective_user.id
        admin_id = int(os.getenv('ADMIN_ID', '0'))

        if user_id != admin_id:
            if hasattr(update, 'callback_query'):
                await update.callback_query.message.reply_text("⛔ This command is restricted to administrators only.")
            else:
                await update.message.reply_text("⛔ This command is restricted to administrators only.")
            return

        # Create keyboard with messaging options
        keyboard = [
            [InlineKeyboardButton("📢 Send Broadcast Message", callback_data='admin_broadcast_message')],
            [InlineKeyboardButton("📨 Send Personalized Message", callback_data='admin_personalized_message')],
            [InlineKeyboardButton("📋 View Message History", callback_data='admin_view_messages')],
            [InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        message = (
            "📨 *ADMIN MESSAGING CENTER*\n\n"
            "Welcome to the messaging center! From here, you can:\n\n"
            "• Send broadcast messages to all donors\n"
            "• Send personalized messages to specific users\n"
            "• View your message history\n\n"
            "Select an option below:"
        )

        if hasattr(update, 'callback_query'):
            await update.callback_query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                message,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )

    except Exception as e:
        logger.error(f"Error showing admin messaging menu: {e}")
        if hasattr(update, 'callback_query'):
            await update.callback_query.message.reply_text(f"Error: {str(e)}")
        else:
            await update.message.reply_text(f"Error: {str(e)}")


# Define conversation states for messaging
BROADCAST_MESSAGE_TEXT = 500
BROADCAST_CONFIRM = 501
PERSONALIZED_USER_ID = 502
PERSONALIZED_MESSAGE_TEXT = 503
PERSONALIZED_CONFIRM = 504


async def admin_broadcast_init(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start broadcast message flow."""
    query = update.callback_query
    await query.answer()

    await query.edit_message_text(
        "📢 *BROADCAST MESSAGE*\n\n"
        "Please enter the message you want to send to all registered donors.\n\n"
        "This message will be sent with the prefix '*IMPORTANT MESSAGE FROM ADMIN*'.\n\n"
        "Type your message now or use /cancel to abort.",
        parse_mode='Markdown'
    )

    return BROADCAST_MESSAGE_TEXT


async def admin_broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Process broadcast message text."""
    message_text = update.message.text
    context.user_data['broadcast_message'] = message_text

    # Show preview and confirmation buttons
    keyboard = [
        [InlineKeyboardButton("✅ Yes, Send to All Donors", callback_data='confirm_broadcast_all')],
        [InlineKeyboardButton("🅰️ Only A+ Donors", callback_data='confirm_broadcast_A+')],
        [InlineKeyboardButton("🅱️ Only B+ Donors", callback_data='confirm_broadcast_B+')],
        [InlineKeyboardButton("🅾️ Only O+ Donors", callback_data='confirm_broadcast_O+')],
        [InlineKeyboardButton("❌ Cancel", callback_data='cancel_broadcast')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "📢 *BROADCAST PREVIEW*\n\n"
        "*IMPORTANT MESSAGE FROM ADMIN*\n\n"
        f"{message_text}\n\n"
        "Please select your target audience or cancel:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    return BROADCAST_CONFIRM


async def admin_broadcast_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle broadcast confirmation."""
    query = update.callback_query
    await query.answer()

    if query.data == 'cancel_broadcast':
        await query.edit_message_text(
            "Broadcast message canceled. No messages were sent.",
            parse_mode='Markdown'
        )
        return ConversationHandler.END

    # Extract target type from callback data
    target_type = 'all'
    if query.data.startswith('confirm_broadcast_'):
        target_parts = query.data.split('_')
        if len(target_parts) > 2:
            target_type = target_parts[2]

    # Get the message
    broadcast_message = context.user_data.get('broadcast_message', 'No message provided')
    formatted_message = f"*IMPORTANT MESSAGE FROM ADMIN*\n\n{broadcast_message}"

    try:
        # Get donors based on target type
        if target_type == 'all':
            donors = db.get_all_donors()
        else:
            # Target specific blood group
            donors = db.get_donors_by_blood_groups([target_type])

        if not donors:
            await query.edit_message_text(
                f"No donors found matching the criteria ({target_type}). No messages were sent.",
                parse_mode='Markdown'
            )
            return ConversationHandler.END

        # Save to database
        broadcast_id = save_broadcast_message(
            admin_id=update.effective_user.id,
            message_text=broadcast_message,
            target_type=target_type
        )

        # Send progress message
        progress_message = await query.edit_message_text(
            f"Sending broadcast to {len(donors)} donors. Please wait...",
            parse_mode='Markdown'
        )

        # Send to all matching donors
        success_count = 0
        for i, donor in enumerate(donors):
            try:
                # Update progress for every 5 donors
                if i % 5 == 0 and i > 0:
                    await context.bot.edit_message_text(
                        chat_id=update.effective_chat.id,
                        message_id=progress_message.message_id,
                        text=f"Sending broadcast... {i}/{len(donors)} completed",
                        parse_mode='Markdown'
                    )

                await context.bot.send_message(
                    chat_id=donor['telegram_id'],
                    text=formatted_message,
                    parse_mode='Markdown'
                )
                success_count += 1

                # Short delay to avoid hitting rate limits
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error sending broadcast to donor {donor['id']}: {e}")
                continue

        # Update database with recipient count
        if broadcast_id:
            update_broadcast_recipient_count(broadcast_id, success_count)

        # Final confirmation
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=progress_message.message_id,
            text=f"✅ Broadcast sent successfully to {success_count}/{len(donors)} donors.\n\n"
                 f"Target group: {target_type}\n"
                 f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            parse_mode='Markdown'
        )

    except Exception as e:
        logger.error(f"Error sending broadcast: {e}")
        await query.edit_message_text(
            f"Error sending broadcast: {str(e)}",
            parse_mode='Markdown'
        )

    # Clear user data
    context.user_data.pop('broadcast_message', None)
    return ConversationHandler.END


async def admin_personalized_init(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start personalized message flow."""
    query = update.callback_query
    await query.answer()

    await query.edit_message_text(
        "📨 *PERSONALIZED MESSAGE*\n\n"
        "Please enter the Telegram ID of the user you want to message.\n\n"
        "You can find user IDs in the donor management section or in support messages.\n\n"
        "Type the ID now or use /cancel to abort.",
        parse_mode='Markdown'
    )

    return PERSONALIZED_USER_ID


async def admin_personalized_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Process user ID for personalized message."""
    try:
        user_id_text = update.message.text.strip()
        user_id = int(user_id_text)

        # Store the user ID
        context.user_data['target_user_id'] = user_id

        # Check if user exists
        donor = db.get_donor_by_telegram_id(user_id)
        user_found = donor is not None

        if user_found:
            context.user_data['target_user_name'] = donor['name']
            await update.message.reply_text(
                f"✅ User found: {donor['name']} (Blood Group: {donor['blood_group']})\n\n"
                f"Please enter the message you want to send to this user.\n\n"
                f"Type your message now or use /cancel to abort.",
                parse_mode='Markdown'
            )
        else:
            # Even if not found as donor, try to send anyway
            await update.message.reply_text(
                f"⚠️ No registered donor found with ID {user_id}, but we'll attempt to send a message anyway.\n\n"
                f"Please enter the message you want to send to this user.\n\n"
                f"Type your message now or use /cancel to abort.",
                parse_mode='Markdown'
            )

        return PERSONALIZED_MESSAGE_TEXT

    except ValueError:
        await update.message.reply_text(
            "❌ Invalid user ID. Please enter a valid numeric Telegram ID.\n\n"
            "Try again or use /cancel to abort.",
            parse_mode='Markdown'
        )
        return PERSONALIZED_USER_ID
    except Exception as e:
        logger.error(f"Error processing user ID: {e}")
        await update.message.reply_text(
            f"Error: {str(e)}\n\nPlease try again or use /cancel to abort.",
            parse_mode='Markdown'
        )
        return PERSONALIZED_USER_ID


async def admin_personalized_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Process personalized message text."""
    message_text = update.message.text
    context.user_data['personalized_message'] = message_text

    user_id = context.user_data.get('target_user_id')
    user_name = context.user_data.get('target_user_name', f"User {user_id}")

    # Show preview and confirmation buttons
    keyboard = [
        [InlineKeyboardButton("✅ Yes, Send Message", callback_data='confirm_personalized')],
        [InlineKeyboardButton("❌ Cancel", callback_data='cancel_personalized')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "📨 *PERSONALIZED MESSAGE PREVIEW*\n\n"
        f"To: {user_name} (ID: {user_id})\n\n"
        f"*PERSONAL MESSAGE FROM ADMIN*\n\n"
        f"{message_text}\n\n"
        "Are you sure you want to send this message?",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    return PERSONALIZED_CONFIRM


async def admin_personalized_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle personalized message confirmation."""
    query = update.callback_query
    await query.answer()

    if query.data == 'cancel_personalized':
        await query.edit_message_text(
            "Personalized message canceled. No message was sent.",
            parse_mode='Markdown'
        )
        return ConversationHandler.END

    # Get the message and user ID
    message_text = context.user_data.get('personalized_message', 'No message provided')
    user_id = context.user_data.get('target_user_id')
    formatted_message = f"*PERSONAL MESSAGE FROM ADMIN*\n\n{message_text}"

    try:
        # Send the message
        await context.bot.send_message(
            chat_id=user_id,
            text=formatted_message,
            parse_mode='Markdown'
        )

        # Save to database
        save_personalized_message(
            admin_id=update.effective_user.id,
            user_id=user_id,
            message_text=message_text
        )

        # Confirmation
        await query.edit_message_text(
            f"✅ Message sent successfully to user ID: {user_id}\n\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            parse_mode='Markdown'
        )

    except Exception as e:
        logger.error(f"Error sending personalized message: {e}")
        await query.edit_message_text(
            f"❌ Error sending message: {str(e)}",
            parse_mode='Markdown'
        )

    # Clear user data
    context.user_data.pop('target_user_id', None)
    context.user_data.pop('target_user_name', None)
    context.user_data.pop('personalized_message', None)
    return ConversationHandler.END


async def admin_view_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """View message history."""
    query = update.callback_query
    await query.answer()

    try:
        # Get recent broadcasts
        broadcasts = get_recent_broadcasts()

        message = "📋 *MESSAGE HISTORY*\n\n"

        if broadcasts:
            message += "*RECENT BROADCASTS:*\n\n"
            for bcast in broadcasts:
                # Truncate message if too long
                msg_preview = bcast['message_text']
                if len(msg_preview) > 50:
                    msg_preview = msg_preview[:47] + "..."

                message += (
                    f"*Date:* {bcast['sent_date']}\n"
                    f"*Target:* {bcast['target_type']}\n"
                    f"*Recipients:* {bcast['recipient_count']}\n"
                    f"*Preview:* {msg_preview}\n"
                    f"---------------------\n"
                )
        else:
            message += "No broadcast messages found.\n\n"

        # Add back button
        keyboard = [
            [InlineKeyboardButton("Send New Broadcast", callback_data='admin_broadcast_message')],
            [InlineKeyboardButton("Send Personalized Message", callback_data='admin_personalized_message')],
            [InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            message,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

    except Exception as e:
        logger.error(f"Error viewing message history: {e}")
        await query.edit_message_text(
            f"Error viewing message history: {str(e)}",
            parse_mode='Markdown'
        )


async def message_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Command handler for direct messaging."""
    try:
        # Check if user is admin
        user_id = update.effective_user.id
        admin_id = int(os.getenv('ADMIN_ID', '0'))

        if user_id != admin_id:
            await update.message.reply_text("⛔ This command is restricted to administrators only.")
            return

        # Display messaging menu
        await admin_messaging_menu(update, context)
    except Exception as e:
        logger.error(f"Error in message command: {e}")
        await update.message.reply_text(f"Error: {str(e)}")


async def debug_admin_messaging(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Debug function to directly access messaging center."""
    # Check if user is admin
    user_id = update.effective_user.id
    admin_id = int(os.getenv('ADMIN_ID', '0'))

    if user_id != admin_id:
        await update.message.reply_text("⛔ Only administrators can access this command.")
        return

    # Create keyboard with messaging options
    keyboard = [
        [InlineKeyboardButton("📢 Send Broadcast Message", callback_data='admin_broadcast_message')],
        [InlineKeyboardButton("📨 Send Personalized Message", callback_data='admin_personalized_message')],
        [InlineKeyboardButton("📋 View Message History", callback_data='admin_view_messages')],
        [InlineKeyboardButton("Back to Dashboard", callback_data='admin_back_to_dashboard')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Send debugging info
    debug_info = (
        "🔍 *ADMIN MESSAGING DEBUG*\n\n"
        f"Admin ID: {admin_id}\n"
        f"User ID: {user_id}\n"
        f"Is Admin: {user_id == admin_id}\n"
        f"Current directory: {os.path.abspath('.')}\n"
        f"DB exists: {os.path.exists(DB_PATH)}\n\n"
        f"Below are the messaging options:"
    )

    await update.message.reply_text(
        debug_info,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

def main():
    application = Application.builder().token(BOT_TOKEN).build()

    # Initialize database tables
    initialize_database()

    # Define error handler
    async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Log errors caused by updates."""
        logger.error(f"Update {update} caused error {context.error}")

        # Send message to the user
        if update and update.effective_message:
            await update.effective_message.reply_text(
                "Sorry, something went wrong. Please try again later."
            )

    # Add debugging handlers
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        debug_message_handler
    ), group=-1)  # Very high priority, just for logging

    # Support conversation handler with highest priority
    support_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("support", support_command),
            CommandHandler("suggest", support_command),
            CommandHandler("feedback", support_command),
            CallbackQueryHandler(open_support, pattern='^open_support$')
        ],
        states={
            SUPPORT_MESSAGE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, support_message)
            ],
            CONFIRM_SEND_SUPPORT: [
                CallbackQueryHandler(support_confirm, pattern='^(confirm|cancel)_support$')
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        persistent=False,  # Set to True only if you have persistence configured
        name="support_conversation",
        allow_reentry=True,
        per_message=False
    )

    # Add the support handler with the highest priority
    application.add_handler(support_conv_handler, group=0)

    # Direct donor registration conversation handler
    direct_donor_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            DIRECT_DONOR_BLOOD_GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, direct_donor_blood_group)],
            DIRECT_DONOR_DIVISION: [MessageHandler(filters.TEXT & ~filters.COMMAND, direct_donor_division)],
            DIRECT_DONOR_DISTRICT: [MessageHandler(filters.TEXT & ~filters.COMMAND, direct_donor_district)],
        },
        fallbacks=[CommandHandler("cancel", help_command)]
    )

    # Post-acceptance donor info collection conversation handler
    post_accept_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_callback, pattern='^accept_donation_terms$')],
        states={
            DONOR_NAME_AFTER_ACCEPT: [MessageHandler(filters.TEXT & ~filters.COMMAND, donor_name_after_accept)],
            DONOR_PHONE_AFTER_ACCEPT: [MessageHandler(filters.TEXT & ~filters.COMMAND, donor_phone_after_accept)],
        },
        fallbacks=[CommandHandler("cancel", help_command)],
        name="post_accept_conversation"
    )

    # Original donor registration conversation handler with per_message=True
    donor_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(button_callback, pattern='^register_donor$'),
            CommandHandler("register", register_command)
        ],
        states={
            DONOR_TERMS_AGREEMENT: [
                CallbackQueryHandler(handle_terms_response, pattern='^(accept|decline)_donor_terms$')
            ],
            DONOR_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, donor_name)],
            DONOR_AGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, donor_age)],
            DONOR_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, donor_phone)],
            DONOR_DIVISION: [MessageHandler(filters.TEXT & ~filters.COMMAND, donor_division)],
            DONOR_DISTRICT: [MessageHandler(filters.TEXT & ~filters.COMMAND, donor_district)],
            DONOR_AREA: [MessageHandler(filters.TEXT & ~filters.COMMAND, donor_area)],
            DONOR_BLOOD_GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, donor_blood_group)],
            DONOR_GENDER: [MessageHandler(filters.TEXT & ~filters.COMMAND, donor_gender)],
            DONOR_TERMS_AFTER_ACCEPT: [
                CallbackQueryHandler(button_callback, pattern='^(accept|decline)_donation_terms$')
            ],
        },
        fallbacks=[CommandHandler("cancel", help_command)],
        per_message=True,
        name="donor_registration"
    )

    # Blood request conversation handler with per_message=True
    request_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(button_callback, pattern='^request_blood$'),
            CommandHandler("request", request_command)
        ],
        states={
            REQUEST_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_name)],
            REQUEST_AGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_age)],
            REQUEST_HOSPITAL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_hospital_name)],
            REQUEST_HOSPITAL_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_hospital_address)],
            REQUEST_AREA: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_area)],
            REQUEST_DIVISION: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_division)],
            REQUEST_DISTRICT: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_district)],
            REQUEST_URGENCY: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_urgency)],
            REQUEST_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_phone)],
            REQUEST_BLOOD_GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_blood_group)],
        },
        fallbacks=[CommandHandler("cancel", help_command)],
        per_message=False,
        name="blood_request"
    )
    # Admin messaging conversation handlers
    broadcast_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_broadcast_init, pattern='^admin_broadcast_message$')],
        states={
            BROADCAST_MESSAGE_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast_text)],
            BROADCAST_CONFIRM: [
                CallbackQueryHandler(admin_broadcast_confirm, pattern='^confirm_broadcast_'),
                CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern='^cancel_broadcast$')
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="admin_broadcast_conversation"
    )

    personalized_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_personalized_init, pattern='^admin_personalized_message$')],
        states={
            PERSONALIZED_USER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_personalized_user_id)],
            PERSONALIZED_MESSAGE_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_personalized_text)],
            PERSONALIZED_CONFIRM: [
                CallbackQueryHandler(admin_personalized_confirm, pattern='^confirm_personalized$'),
                CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern='^cancel_personalized$')
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="admin_personalized_conversation"
    )

    # Add these handlers to your application
    application.add_handler(broadcast_conv_handler, group=1)
    application.add_handler(personalized_conv_handler, group=1)

    # Add command handler for admin messaging
    application.add_handler(CommandHandler("message", admin_messaging_menu))
    # Admin reply conversation handler
    admin_reply_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("reply", admin_reply_to_user)],
        states={
            ADMIN_REPLY_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reply_message)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="admin_reply_conversation"
    )

    # Add the rest of conversation handlers - AFTER the support handler
    application.add_handler(direct_donor_conv_handler, group=1)
    application.add_handler(post_accept_conv_handler, group=1)
    application.add_handler(donor_conv_handler, group=1)
    application.add_handler(request_conv_handler, group=1)
    application.add_handler(admin_reply_conv_handler, group=1)
    # Add the admin search input handler with lower priority than the conversation handlers
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.UpdateType.MESSAGE,
        admin_search_input
    ), group=10)  # Much lower priority than conversation handlers

    # Add command handlers
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("donors", donors_command))
    application.add_handler(CommandHandler("requests", requests_command))
    application.add_handler(CommandHandler("mydashboard", donor_dashboard))
    application.add_handler(CommandHandler("mystats", donor_dashboard))
    application.add_handler(CommandHandler("msgdebug", debug_admin_messaging))
    application.add_handler(CommandHandler("message", message_command))


    # Admin command handlers
    application.add_handler(CommandHandler("admin", admin_dashboard))
    application.add_handler(CommandHandler("dashboard", admin_dashboard))
    application.add_handler(CommandHandler("stats", admin_stats_command))
    application.add_handler(CommandHandler("operations", admin_operation_list_command))

    # Only add these if the functions are actually defined
    if 'admin_manage_requests' in globals():
        application.add_handler(CommandHandler("manage_requests", admin_manage_requests))
    if 'admin_manage_users' in globals():
        application.add_handler(CommandHandler("manage_users", admin_manage_users))
    if 'admin_settings' in globals():
        application.add_handler(CommandHandler("settings", admin_settings))
    if 'admin_database_backup' in globals():
        application.add_handler(CommandHandler("backup", admin_database_backup))

    # Add specific callback handlers
    application.add_handler(CallbackQueryHandler(show_main_menu, pattern='^show_main_menu$'))
    application.add_handler(CallbackQueryHandler(lambda update, context: refresh_donor_dashboard(update),
                                                 pattern='^refresh_donor_dashboard$'))
    application.add_handler(CallbackQueryHandler(lambda update, context: refresh_donor_dashboard(update),
                                                 pattern='^open_donor_dashboard$'))

    # Update the callback handler to include admin_view_support
    application.add_handler(CallbackQueryHandler(admin_view_support_messages, pattern='^admin_view_support$'))
    application.add_handler(CallbackQueryHandler(admin_mark_support_read, pattern='^admin_mark_support_read$'))
    application.add_handler(CallbackQueryHandler(send_thanks, pattern='^send_thanks$'))

    # Admin callback handlers - these should be before the general button_callback
    application.add_handler(CallbackQueryHandler(handle_admin_callbacks, pattern='^admin_'))

    # Add general callback handler last (catch-all)
    application.add_handler(CallbackQueryHandler(button_callback))

    # Add unhandled message handler to help with debugging
    application.add_handler(MessageHandler(filters.ALL, unhandled_message), group=999)

    # Add error handler
    application.add_error_handler(error_handler)

    # Start the Bot
    application.run_polling()

if __name__ == '__main__':
    main()
