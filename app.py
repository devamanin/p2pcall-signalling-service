import gevent.monkey
gevent.monkey.patch_all()

import math
from flask import Flask, request
from flask_socketio import SocketIO, emit, join_room, leave_room
import uuid
import time
import threading
import os
import json
import requests as http_requests
import firebase_admin
from firebase_admin import credentials, messaging

app = Flask(__name__)
# Using gevent for production compatibility with gunicorn on Render
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

# Initialize Firebase Admin
service_account_info = os.environ.get('FIREBASE_SERVICE_ACCOUNT')
service_account_path = os.path.join(os.path.dirname(__file__), 'service-account.json')

firebase_app = None
_firebase_project_id = None

if service_account_info:
    try:
        sa_info = json.loads(service_account_info)
        cred = credentials.Certificate(sa_info)
        firebase_app = firebase_admin.initialize_app(cred)
        _firebase_project_id = sa_info.get('project_id')
        print("[Firebase] Admin SDK initialized via environment variable")
    except Exception as e:
        print(f"[Firebase] Error initializing Admin SDK from ENV: {e}")
elif os.path.exists(service_account_path):
    try:
        with open(service_account_path) as f:
            sa_info = json.load(f)
        cred = credentials.Certificate(sa_info)
        firebase_app = firebase_admin.initialize_app(cred)
        _firebase_project_id = sa_info.get('project_id')
        print(f"[Firebase] Admin SDK initialized via {service_account_path}")
    except Exception as e:
        print(f"[Firebase] Error initializing Admin SDK from file: {e}")
else:
    print("[Firebase] No credentials found (FIREBASE_SERVICE_ACCOUNT or service-account.json). Push notifications will be disabled.")


def _get_fcm_token_via_rest(target_uid):
    """Fetch a user's FCM token via Firestore REST API (avoids gRPC/gevent conflicts)."""
    if not _firebase_project_id or not firebase_app:
        return None
    try:
        # Get an access token from the Firebase Admin credential
        access_token = firebase_app.credential.get_access_token().access_token
        url = (f'https://firestore.googleapis.com/v1/projects/{_firebase_project_id}'
               f'/databases/(default)/documents/users/{target_uid}')
        headers = {'Authorization': f'Bearer {access_token}'}
        params = {'mask.fieldPaths': 'fcmToken'}  # Only fetch the field we need
        resp = http_requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code == 200:
            fields = resp.json().get('fields', {})
            token_field = fields.get('fcmToken', {})
            return token_field.get('stringValue')
        elif resp.status_code == 404:
            print(f"[Firestore REST] User {target_uid[:8]} not found")
            return None
        else:
            print(f"[Firestore REST] Error {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"[Firestore REST] Exception: {e}")
        return None

# rooms: room_id -> {id, offer, status, createdBy, joinedBy, metadata, createdAt}

rooms = {}

# previous_matches: sid -> last_partner_sid (to prevent immediate re-matching)
previous_matches = {}

# Reverse lookup: sid -> set of room_ids they are in (creator or joiner)
user_rooms = {}

# Lock for thread-safe room operations
rooms_lock = threading.Lock()

# Region mapping for absolute location matching
REGION_MAP = {
    'North America': ['USA', 'Canada', 'Mexico', 'United States', 'US'],
    'Europe': ['UK', 'United Kingdom', 'France', 'Germany', 'Italy', 'Spain', 'Netherlands', 'Sweden', 'Norway', 'Denmark', 'Ireland', 'Switzerland', 'Austria', 'Belgium', 'Poland', 'Portugal', 'Russia'],
    'Asia': ['India', 'Japan', 'China', 'South Korea', 'Singapore', 'Thailand', 'Vietnam', 'Indonesia', 'Malaysia', 'Philippines', 'Pakistan', 'Bangladesh', 'Sri Lanka', 'UAE', 'Saudi Arabia', 'Israel'],
    'South America': ['Brazil', 'Argentina', 'Colombia', 'Peru', 'Chile', 'Ecuador', 'Venezuela', 'Bolivia', 'Paraguay', 'Uruguay'],
    'Oceania': ['Australia', 'New Zealand', 'Fiji', 'Papua New Guinea'],
    'Africa': ['Nigeria', 'Egypt', 'South Africa', 'Kenya', 'Morocco', 'Ethiopia', 'Ghana']
}

def haversine(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return float('inf')
    R = 6371.0 # Earth radius in kilometers
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def _get_region_from_location(location_str):
    """Determine the region from a 'City, Country' string."""
    if not location_str or location_str == 'Global':
        return 'Global'
    
    # Extract country name (usually after the comma)
    country = location_str.split(',')[-1].strip().lower()
    
    for region, countries in REGION_MAP.items():
        for c in countries:
            c_low = c.lower()
            if c_low == country or c_low in country or country in c_low:
                return region
    return 'Global'

# Max age for a 'waiting' room before it's considered stale (seconds)
STALE_ROOM_TTL = 60

# Max age for a 'private' room that hasn't been joined yet
PRIVATE_ROOM_TTL = 600 # 10 minutes

# ICE_SERVERS = {"iceServers":[{"urls":["stun:stun.cloudflare.com:3478","stun:stun.cloudflare.com:53"]},{"urls":["turn:turn.cloudflare.com:3478?transport=udp","turn:turn.cloudflare.com:3478?transport=tcp","turns:turn.cloudflare.com:5349?transport=tcp","turn:turn.cloudflare.com:53?transport=udp","turn:turn.cloudflare.com:80?transport=tcp","turns:turn.cloudflare.com:443?transport=tcp"],"username":"g0e86ec05b94407fb8406184609716f2a5196dee9125367456a0f73dc5516aa8","credential":"c4d78514269bce94108fbc7cc080eea6b060fcc92aabc18cc97e74e226829f4b"}]}
ICE_SERVERS = {
  "iceServers": [
    {
      "urls": ["stun:stun.cloudflare.com:3478", "stun:stun.l.google.com:19302"]
    },
    {
      # Your Cloudflare TURN
      "urls": [
        "turn:turn.cloudflare.com:3478?transport=udp",
        "turn:turn.cloudflare.com:3478?transport=tcp",
        "turns:turn.cloudflare.com:443?transport=tcp"
      ],
      "username": "g0e86ec05b94407fb8406184609716f2a5196dee9125367456a0f73dc5516aa8",
      "credential": "c4d78514269bce94108fbc7cc080eea6b060fcc92aabc18cc97e74e226829f4b"
    },
    {
      # BACKUP PUBLIC TURN SERVER (Metered)
      "urls": [
        "turn:openrelay.metered.ca:80",
        "turn:openrelay.metered.ca:443",
        "turn:openrelay.metered.ca:443?transport=tcp"
      ],
      "username": "openrelayproject",
      "credential": "openrelayproject",
      "credentialType": "password"
    }
  ]
}

# --- Cloudflare Dynamic TURN Credentials ---
TURN_KEY_ID = os.environ.get("CLOUDFLARE_TURN_KEY_ID")
TURN_API_TOKEN = os.environ.get("CLOUDFLARE_TURN_API_TOKEN")

CACHED_ICE_SERVERS = None
CACHED_ICE_EXPIRY = 0

def get_dynamic_ice_servers():
    global CACHED_ICE_SERVERS, CACHED_ICE_EXPIRY
    
    # Use cached credentials if valid
    now = time.time()
    if CACHED_ICE_SERVERS and now < CACHED_ICE_EXPIRY:
        print("[Server] Returning cached Cloudflare TURN credentials.")
        return CACHED_ICE_SERVERS
        
    # If no API keys are provided, fallback to the static dictionary
    if not TURN_KEY_ID or not TURN_API_TOKEN:
        print("[Server] Warning: CLOUDFLARE_TURN_KEY_ID or API_TOKEN not set! Falling back to static ICE_SERVERS.")
        return ICE_SERVERS
        
    try:
        print("[Server] Generating fresh Cloudflare TURN credentials...")
        url = f"https://rtc.live.cloudflare.com/v1/turn/keys/{TURN_KEY_ID}/credentials/generate-ice-servers"
        headers = {
            "Authorization": f"Bearer {TURN_API_TOKEN}",
            "Content-Type": "application/json"
        }
        # TTL is 24 hours
        payload = {"ttl": 86400}
        
        response = http_requests.post(url, headers=headers, json=payload, timeout=5)
        
        if response.status_code in [200, 201]:
            data = response.json()
            
            # Extract iceServers array from response
            ice_servers_list = []
            if 'result' in data and 'iceServers' in data['result']:
                ice_servers_list = data['result']['iceServers']
            elif 'iceServers' in data:
                ice_servers_list = data['iceServers']
            else:
                ice_servers_list = [data] # fallback
                
            # Add Google STUN as fallback
            ice_servers_list.insert(0, {
                "urls": ["stun:stun.l.google.com:19302"]
            })
            
            CACHED_ICE_SERVERS = {
                "iceServers": ice_servers_list,
                "iceCandidatePoolSize": 10
            }
            # Cache for 12 hours (half of the TTL)
            CACHED_ICE_EXPIRY = now + (86400 / 2) 
            return CACHED_ICE_SERVERS
        else:
            print(f"[Server] Failed to generate TURN credentials: {response.status_code} - {response.text}")
            return ICE_SERVERS
            
    except Exception as e:
        print(f"[Server] Error fetching TURN credentials: {e}")
        return ICE_SERVERS



def _track_user_room(sid, room_id):
    """Track that a user is associated with a room."""
    if sid not in user_rooms:
        user_rooms[sid] = set()
    user_rooms[sid].add(room_id)


def _untrack_user_room(sid, room_id):
    """Remove room association for a user."""
    if sid in user_rooms:
        user_rooms[sid].discard(room_id)
        if not user_rooms[sid]:
            del user_rooms[sid]


def _destroy_room(room_id, reason="unknown"):
    """Fully destroy a room, notifying remaining participants."""
    if room_id not in rooms:
        return
    
    room = rooms[room_id]
    print(f"[Server] Destroying room {room_id[:8]}... reason={reason}")
    
    # Notify everyone in the room
    emit('session_ended', {'room_id': room_id, 'reason': reason}, to=room_id, namespace='/')
    
    # Untrack both participants
    creator = room.get('createdBy')
    joiner = room.get('joinedBy')
    if creator:
        _untrack_user_room(creator, room_id)
    if joiner:
        _untrack_user_room(joiner, room_id)
    
    del rooms[room_id]


def _cleanup_user_rooms(sid, keep_room_id=None):
    """Clean up ALL rooms associated with a user, optionally keeping one."""
    if sid not in user_rooms:
        return
    room_ids = list(user_rooms.get(sid, set()))
    for rid in room_ids:
        if rid == keep_room_id:
            continue
        if rid in rooms:
            _destroy_room(rid, reason=f"user {sid[:8]} cleanup")


def _purge_stale_rooms():
    """Remove rooms that are older than their respective TTLs."""
    now = time.time()
    stale = []
    for room_id, room in rooms.items():
        if room['status'] == 'waiting' and (now - room.get('createdAt', 0)) > STALE_ROOM_TTL:
            stale.append(room_id)
        elif room['status'] == 'private' and (now - room.get('createdAt', 0)) > PRIVATE_ROOM_TTL:
            stale.append(room_id)
    
    for rid in stale:
        if rid in rooms:
            print(f"[Server] Purging stale room {rid[:8]}... (age: {now - rooms[rid].get('createdAt', 0):.0f}s)")
            _destroy_room(rid, reason="stale_room_purged")

    if stale:        print(f"[Server] Purged {len(stale)} stale rooms. Active rooms: {len(rooms)}")


@socketio.on('get_ice_servers')
def handle_get_ice_servers(data):
    print(f"ICE servers requested by {request.sid[:8]}")
    return get_dynamic_ice_servers()


@socketio.on('connect')
def handle_connect():
    print(f"[Server] Client connected: {request.sid[:8]}")


@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    print(f"[Server] Client disconnected: {sid[:8]}")
    
    # Clean up rooms this user was in
    if sid in user_rooms:
        room_ids = list(user_rooms[sid])
        for room_id in room_ids:
            if room_id in rooms:
                room = rooms[room_id]
                
                # If it's a private room, do NOT destroy it instantly.
                # Mobile sockets flicker, and User A needs the room to stay alive while User B joins.
                if room['status'] in ['private', 'occupied']:
                    print(f"[Server] Client {sid[:8]} disconnected from private room {room_id[:8]}. Keeping room alive.")
                    leave_room(room_id)
                    # Notify the other person if the room is occupied
                    emit('peer_disconnected', {'sid': sid}, to=room_id, include_self=False)
                    continue

                # For random matching rooms, destroy instantly
                print(f"[Server] Cleaning room {room_id[:8]} due to disconnect of {sid[:8]} "
                      f"(role={'creator' if room['createdBy'] == sid else 'joiner'})")
                leave_room(room_id)
                _destroy_room(room_id, reason="participant_disconnected")
        
        # Final cleanup of tracking association for this specific SID
        if sid in user_rooms:
            del user_rooms[sid]
        # We don't delete from previous_matches here as that's used for matching logic

@socketio.on('notify_call')
def handle_notify_call(data):
    if firebase_app is None:
        return {'success': False, 'message': 'Firebase Admin not initialized'}
    
    target_uid = data.get('target_uid')
    caller_name = data.get('caller_name', 'Someone')
    room_id = data.get('room_id')
    caller_photo_url = data.get('caller_photo_url')
    caller_uid = data.get('caller_uid', '')  # Real Firebase UID from client

    print(f"[Server] Notification requested for {target_uid[:8]} from {caller_name} (uid: {caller_uid[:8] if caller_uid else 'N/A'})")

    # Run Firebase operations in a background task to prevent blocking the signaling loop
    socketio.start_background_task(_send_call_notification, target_uid, caller_name, room_id, caller_photo_url, caller_uid)
    
    return {'success': True, 'message': 'Notification queued'}

def _send_call_notification(target_uid, caller_name, room_id, caller_photo_url, caller_uid):
    try:
        # 1. Fetch target user's FCM token via REST (avoids gRPC/eventlet conflicts)
        fcm_token = _get_fcm_token_via_rest(target_uid)
        if not fcm_token:
            print(f"[Server] Notification FAILED: User {target_uid[:8]} has no FCM token or not found")
            return

        import datetime
        # 2. Send Push Notification via FCM (High Priority)
        message = messaging.Message(
            notification=messaging.Notification(
                title='Incoming Video Call',
                body=f'{caller_name} is calling you...',
                image=caller_photo_url,
            ),
            data={
                'type': 'video_call',
                'caller_name': caller_name,
                'caller_uid': caller_uid or '',
                'room_id': room_id,
                'caller_photo_url': caller_photo_url or '',
                'click_action': 'FLUTTER_NOTIFICATION_CLICK',
                'id': room_id, # Some plugins use id at root level
            },
            android=messaging.AndroidConfig(
                priority='high',
                ttl=datetime.timedelta(seconds=45),
                notification=messaging.AndroidNotification(
                    channel_id='high_importance_channel',
                    tag=target_uid, # Replaces existing notification for this user
                    priority='max',
                    sound='default',
                    visibility='public',
                ),
            ),
            apns=messaging.APNSConfig(
                headers={'apns-priority': '10'},
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        content_available=True,
                        sound='default',
                        category='INCOMING_CALL'
                    )
                ),
            ),
            token=fcm_token,
        )

        response = messaging.send(message)
        print(f"[Server] Successfully sent FCM message: {response}")

    except Exception as e:
        print(f"[Server] Error sending FCM background task: {e}")

@socketio.on('cancel_call')
def handle_cancel_call(data):
    if firebase_app is None:
        return {'success': False, 'message': 'Firebase Admin not initialized'}
    
    target_uid = data.get('target_uid')
    print(f"[Server] Cancel call requested for {target_uid[:8]}")

    socketio.start_background_task(_send_cancel_notification, target_uid)
    return {'success': True, 'message': 'Cancel notification queued'}

def _send_cancel_notification(target_uid):
    try:
        fcm_token = _get_fcm_token_via_rest(target_uid)
        if not fcm_token:
            return

        message = messaging.Message(
            data={'type': 'cancel_call'},
            android=messaging.AndroidConfig(priority='high'),
            apns=messaging.APNSConfig(
                headers={'apns-priority': '10'},
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(content_available=True)
                ),
            ),
            token=fcm_token,
        )

        messaging.send(message)
        print(f"[Server] Successfully sent Cancel FCM message")
    except Exception as e:
        print(f"[Server] Error sending Cancel FCM task: {e}")

@socketio.on('find_room')
def handle_find_room(data):
    my_metadata = data.get('metadata', {})
    sid = request.sid
    now = time.time()
    
    # First, purge stale rooms
    _purge_stale_rooms()
    
    print(f"[Server] User {sid[:8]} searching. Active rooms: {len(rooms)}")
    
    my_client_id = my_metadata.get('client_id')
    # Get last partner to avoid immediate re-match
    last_partner = previous_matches.get(my_client_id) if my_client_id else previous_matches.get(sid)
    
    candidates = []

    for room_id, room in rooms.items():
        # Must be waiting, not created by me, and not stale
        if room['status'] != 'waiting':
            continue
        
        room_creator_client_id = room.get('metadata', {}).get('client_id')
        if room['createdBy'] == sid or (room_creator_client_id and room_creator_client_id == my_client_id):
            continue
            
        if (now - room.get('createdAt', 0)) > STALE_ROOM_TTL:
            continue
            
        # Anti-repeat check (Penalize score instead of skipping to allow matching if only 2 users)
        is_previous_match = False
        if room_creator_client_id and room_creator_client_id == last_partner:
            is_previous_match = True
        elif not room_creator_client_id and room['createdBy'] == last_partner:
            is_previous_match = True

        room_meta = room.get('metadata', {})
        
        # Match Mode matching (Hard requirement)
        my_match_mode = my_metadata.get('matchMode', 'virtual')
        room_match_mode = room_meta.get('matchMode', 'virtual')
        if my_match_mode != room_match_mode:
            continue
        
        # Gender matching (Hard requirement)
        if my_metadata.get('targetGender') != 'Any Gender':
            if room_meta.get('myGender') != my_metadata.get('targetGender'):
                continue
        
        if room_meta.get('targetGender') != 'Any Gender':
            if my_metadata.get('myGender') != room_meta.get('targetGender'):
                continue
        
        # 3. Absolute Location Matching (Hard requirement)
        my_location_filters = my_metadata.get('locations', ['Global'])
        creator_physical_region = _get_region_from_location(room_meta.get('location'))
        
        # If I have a specific filter, the creator MUST be in that region
        if 'Global' not in my_location_filters:
            if creator_physical_region not in my_location_filters:
                continue
        
        # Check if room creator is okay with my physical region
        creator_location_filters = room_meta.get('locations', ['Global'])
        my_physical_region = _get_region_from_location(my_metadata.get('location'))
        if 'Global' not in creator_location_filters:
            if my_physical_region not in creator_location_filters:
                continue

        # 4. Interests Matching (Preference/Soft requirement)
        my_interests = my_metadata.get('interests', [])
        room_interests = room_meta.get('interests', [])
        
        # Calculate Match Score (Soft requirements)
        score = 0
        
        if is_previous_match:
            score -= 1000

        
        # Distance calculation
        distance = float('inf')
        match_mode = my_metadata.get('matchMode', 'virtual')
        
        if match_mode == 'real' or room_meta.get('matchMode') == 'real':
            my_lat = my_metadata.get('latitude')
            my_lon = my_metadata.get('longitude')
            room_lat = room_meta.get('latitude')
            room_lon = room_meta.get('longitude')
            distance = haversine(my_lat, my_lon, room_lat, room_lon)
        
        # Shared interests bonus (High priority)
        if my_interests and room_interests:
            shared_count = len(set(my_interests).intersection(set(room_interests)))
            score += shared_count * 20 # Significant boost for shared interests
        
        # Exact Location bonus
        if my_metadata.get('location') == room_meta.get('location') and my_metadata.get('location') != 'Global':
            score += 10
            
        candidates.append({
            'room_id': room_id,
            'score': score,
            'distance': distance,
            'createdAt': room.get('createdAt', 0)
        })

    if not candidates:
        print(f"[Server] No room found for {sid[:8]}")
        return {'room_id': None}
        
    # Sort by score (descending) and then by age (oldest first)
    # This ensures "Magic Matches" are picked first, but fallback happens naturally
    if my_metadata.get('matchMode') == 'real':
        # For real mode, sort primarily by distance (ascending), then score (descending), then age (oldest first)
        candidates.sort(key=lambda x: (x['distance'], -x['score'], -x['createdAt']))
    else:
        # For virtual mode, sort by score (descending), then age (oldest first)
        candidates.sort(key=lambda x: (-x['score'], -x['createdAt']))
    
    best_match = candidates[0]
    print(f"[Server] Best match for {sid[:8]} is {best_match['room_id'][:8]} with score {best_match['score']}")
    
    return {'room_id': best_match['room_id']}


@socketio.on('create_room')
def handle_create_room(data):
    sid = request.sid
    
    # IMPORTANT: Clean up any existing waiting rooms by this user first
    # This prevents ghost sessions from accumulating
    if sid in user_rooms:
        existing_rooms = list(user_rooms[sid])
        for rid in existing_rooms:
            if rid in rooms and rooms[rid]['status'] == 'waiting' and rooms[rid]['createdBy'] == sid:
                print(f"[Server] Cleaning old waiting room {rid[:8]} before creating new one for {sid[:8]}")
                leave_room(rid)
                creator = rooms[rid].get('createdBy')
                if creator:
                    _untrack_user_room(creator, rid)
                del rooms[rid]
    
    room_id = str(uuid.uuid4())
    rooms[room_id] = {
        'id': room_id,
        'offer': data['offer'],
        'status': 'waiting',
        'createdBy': sid,
        'joinedBy': None,
        'metadata': data.get('metadata', {}),
        'createdAt': time.time()
    }
    join_room(room_id)
    _track_user_room(sid, room_id)
    
    print(f"[Server] Room created by {sid[:8]}: {room_id[:8]}. Active rooms: {len(rooms)}")
    return {'room_id': room_id}


@socketio.on('join_room')
def handle_join_room(data):
    room_id = data['room_id']
    sid = request.sid
    print(f"[Server] Client {sid[:8]} attempting to join room: {room_id[:8]}")
    
    if room_id not in rooms:
        print(f"[Server] Join FAILED: room {room_id[:8]} does not exist")
        return {'success': False, 'message': 'Room does not exist'}
    
    room = rooms[room_id]
    
    if room['status'] != 'waiting':
        print(f"[Server] Join FAILED: room {room_id[:8]} status is '{room['status']}' (not waiting)")
        return {'success': False, 'message': 'Room is already occupied'}
    
    if room['createdBy'] == sid:
        print(f"[Server] Join FAILED: {sid[:8]} cannot join own room")
        return {'success': False, 'message': 'Cannot join own room'}
    
    # IMMEDIATELY mark as occupied to prevent race conditions
    room['status'] = 'occupied'
    room['joinedBy'] = sid
    
    # Track as previous match for both parties
    creator_client_id = room.get('metadata', {}).get('client_id')
    my_client_id = data.get('metadata', {}).get('client_id')
    
    if my_client_id and creator_client_id:
        previous_matches[my_client_id] = creator_client_id
        previous_matches[creator_client_id] = my_client_id
    else:
        creator_sid = room['createdBy']
        previous_matches[sid] = creator_sid
        previous_matches[creator_sid] = sid
    
    join_room(room_id)
    _track_user_room(sid, room_id)
    
    # Notify the creator that someone joined
    emit('peer_joined', {'metadata': data.get('metadata', {})}, to=room_id, include_self=False)
    
    print(f"[Server] Client {sid[:8]} joined room {room_id[:8]}. Room is now OCCUPIED. "
          f"(creator={room['createdBy'][:8]}, joiner={sid[:8]})")
    return {'success': True, 'offer': room['offer'], 'metadata': room['metadata']}


@socketio.on('create_private_room')
def handle_create_private_room(data):
    sid = request.sid
    room_id = str(uuid.uuid4())
    rooms[room_id] = {
        'id': room_id,
        'offer': data['offer'],
        'status': 'private',
        'createdBy': sid,
        'joinedBy': None,
        'metadata': data.get('metadata', {}),
        'createdAt': time.time()
    }
    join_room(room_id)
    _track_user_room(sid, room_id)
    
    print(f"[Server] Private Room CREATED: {room_id} (creator={sid[:8]})")
    return {'room_id': room_id}


@socketio.on('join_private_room')
def handle_join_private_room(data):
    room_id = str(data.get('room_id', '')).strip()
    sid = request.sid
    print(f"[Server] Client {sid[:8]} ATTEMPTING to join private room: {room_id}")
    
    if not room_id:
        print(f"[Server] Private Join FAILED: No room_id provided")
        return {'success': False, 'message': 'No room_id provided'}

    if room_id not in rooms:
        active_rooms = list(rooms.keys())
        print(f"[Server] Private Join FAILED: room {room_id} not found. Active rooms: {active_rooms}")
        return {'success': False, 'message': f'Room not found on server. Active count: {len(active_rooms)}'}
    
    room = rooms[room_id]
    
    # Allow joining if private or if already occupied (re-join)
    if room['status'] not in ['private', 'occupied']:
        print(f"[Server] Private Join FAILED: room {room_id[:8]} status is {room['status']}")
        return {'success': False, 'message': f'Room status is {room["status"]}'}
    
    room['status'] = 'occupied'
    room['joinedBy'] = sid
    
    join_room(room_id)
    _track_user_room(sid, room_id)
    
    # Notify the creator that someone joined
    emit('peer_joined', {'metadata': data.get('metadata', {})}, to=room_id, include_self=False)
    
    print(f"[Server] Client {sid[:8]} SUCCESSFULLY joined room {room_id[:8]}")
    return {'success': True, 'offer': room['offer'], 'metadata': room['metadata']}


@socketio.on('send_answer')
def handle_send_answer(data):
    room_id = data['room_id']
    if room_id in rooms:
        print(f"[Server] Answer relayed in room {room_id[:8]} from {request.sid[:8]}")
        emit('answer', {'answer': data['answer']}, to=room_id, include_self=False)
    else:
        print(f"[Server] Answer for non-existent room {room_id[:8]}")


@socketio.on('send_candidate')
def handle_send_candidate(data):
    room_id = data['room_id']
    if room_id in rooms:
        emit('candidate', {'candidate': data['candidate']}, to=room_id, include_self=False)


@socketio.on('send_message')
def handle_send_message(data):
    room_id = data['room_id']
    emit('message', {'text': data['text'], 'sender': request.sid}, to=room_id, include_self=False)


@socketio.on('typing_status')
def handle_typing_status(data):
    room_id = data['room_id']
    emit('typing', {'isTyping': data['isTyping']}, to=room_id, include_self=False)


@socketio.on('leave_room')
def handle_leave_room(data):
    room_id = data.get('room_id')
    if not room_id:
        return
    
    sid = request.sid
    print(f"[Server] Client {sid[:8]} leaving room {room_id[:8]}")
    
    if room_id in rooms:
        _destroy_room(room_id, reason=f"leave by {sid[:8]}")
    else:
        # Fallback if room already gone but socket still in room
        leave_room(room_id)
        _untrack_user_room(sid, room_id)


@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    print(f"[Server] Client disconnected: {sid[:8]}")
    
    if sid in user_rooms:
        rooms_to_cleanup = list(user_rooms[sid])
        for room_id in rooms_to_cleanup:
            if room_id in rooms:
                _destroy_room(room_id, reason="peer_disconnected")
            else:
                _untrack_user_room(sid, room_id)


@app.route('/admob-reward', methods=['GET'])
def admob_reward():
    # AdMob SSV parameters:
    # ad_network, ad_unit, reward_amount, reward_item, timestamp, transaction_id, user_id, signature, key_id
    ad_unit = request.args.get('ad_unit')
    reward_amount = request.args.get('reward_amount')
    user_id = request.args.get('user_id')
    transaction_id = request.args.get('transaction_id')
    
    print(f"AdMob Reward Callback: User {user_id} earned {reward_amount} from {ad_unit}. Transaction: {transaction_id}")
    
    # IMPORTANT: In a production app, you MUST verify the signature here using Google's public keys
    # to ensure the request actually came from AdMob and wasn't spoofed.
    # You should also check if the transaction_id has already been processed to prevent replay attacks.
    
    if user_id:
        # Here you would update your DATABASE:
        # db.users.update_one({'id': user_id}, {'$inc': {'coins': int(reward_amount)}})
        return "OK", 200
    
    return "Missing user_id", 400


@app.route('/health', methods=['GET'])
def health_check():
    active = len(rooms)
    waiting = sum(1 for r in rooms.values() if r['status'] == 'waiting')
    occupied = sum(1 for r in rooms.values() if r['status'] == 'occupied')
    return f"OK | rooms: {active} (waiting: {waiting}, occupied: {occupied})", 200


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
