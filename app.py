from flask import Flask, request
from flask_socketio import SocketIO, emit, join_room, leave_room
import uuid
import time
import threading

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# rooms: room_id -> { offer, status, createdBy, joinedBy, metadata, createdAt }
rooms = {}

# Reverse lookup: sid -> set of room_ids they are in (creator or joiner)
user_rooms = {}

# Lock for thread-safe room operations
rooms_lock = threading.Lock()

# Max age for a 'waiting' room before it's considered stale (seconds)
STALE_ROOM_TTL = 30

ICE_SERVERS = {
    "iceServers": [
      {
        "urls": [
          "stun:stun.cloudflare.com:3478",
          "stun:stun.cloudflare.com:53",
        ],
      },
      {
        "urls": [
          "turn:turn.cloudflare.com:3478?transport=udp",
          "turn:turn.cloudflare.com:3478?transport=tcp",
          "turns:turn.cloudflare.com:5349?transport=tcp",
          "turn:turn.cloudflare.com:53?transport=udp",
          "turn:turn.cloudflare.com:80?transport=tcp",
          "turns:turn.cloudflare.com:443?transport=tcp",
        ],
        "username": "g0d1c991d1988319f2a1e57d8407530631be587513304980f222bbe90f879da5",
        "credential": "0308d1e94c15c822aa900cdae2fd2eaa2ffe42ab0123ca49978e86f72e1a9f6d",
      },
    ],
}


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
    """Remove waiting rooms that are older than STALE_ROOM_TTL."""
    now = time.time()
    stale = []
    for room_id, room in rooms.items():
        if room['status'] == 'waiting' and (now - room.get('createdAt', 0)) > STALE_ROOM_TTL:
            stale.append(room_id)
    
    for rid in stale:
        if rid in rooms:
            print(f"[Server] Purging stale room {rid[:8]}... (age: {now - rooms[rid].get('createdAt', 0):.0f}s)")
            creator = rooms[rid].get('createdBy')
            if creator:
                _untrack_user_room(creator, rid)
            del rooms[rid]
    
    if stale:
        print(f"[Server] Purged {len(stale)} stale rooms. Active rooms: {len(rooms)}")


@socketio.on('get_ice_servers')
def handle_get_ice_servers(data):
    print(f"ICE servers requested by {request.sid[:8]}")
    return ICE_SERVERS


@socketio.on('connect')
def handle_connect():
    print(f"[Server] Client connected: {request.sid[:8]}")


@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    print(f"[Server] Client disconnected: {sid[:8]}")
    
    # Clean up ALL rooms this user was in (as creator or joiner)
    if sid in user_rooms:
        room_ids = list(user_rooms[sid])
        for room_id in room_ids:
            if room_id in rooms:
                room = rooms[room_id]
                print(f"[Server] Cleaning room {room_id[:8]} due to disconnect of {sid[:8]} "
                      f"(role={'creator' if room['createdBy'] == sid else 'joiner'})")
                leave_room(room_id)
                _destroy_room(room_id, reason="participant_disconnected")
        
        # Final cleanup of tracking
        if sid in user_rooms:
            del user_rooms[sid]


@socketio.on('find_room')
def handle_find_room(data):
    my_metadata = data.get('metadata', {})
    sid = request.sid
    now = time.time()
    
    # First, purge stale rooms
    _purge_stale_rooms()
    
    print(f"[Server] User {sid[:8]} searching. Active rooms: {len(rooms)}, "
          f"waiting: {sum(1 for r in rooms.values() if r['status'] == 'waiting')}")
    
    for room_id, room in rooms.items():
        # Must be waiting, not created by me, and not stale
        if room['status'] != 'waiting':
            continue
        if room['createdBy'] == sid:
            continue
        if (now - room.get('createdAt', 0)) > STALE_ROOM_TTL:
            continue
            
        room_meta = room.get('metadata', {})
        
        # Gender matching
        if my_metadata.get('targetGender') != 'Any Gender':
            if room_meta.get('myGender') != my_metadata.get('targetGender'):
                continue
        
        if room_meta.get('targetGender') != 'Any Gender':
            if my_metadata.get('myGender') != room_meta.get('targetGender'):
                continue
        
        print(f"[Server] Matched {sid[:8]} with room {room_id[:8]} "
              f"(creator: {room['createdBy'][:8]}, age: {now - room.get('createdAt', 0):.1f}s)")
        return {'room_id': room_id}
    
    print(f"[Server] No room found for {sid[:8]}")
    return {'room_id': None}


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
    
    join_room(room_id)
    _track_user_room(sid, room_id)
    
    # Notify the creator that someone joined
    emit('peer_joined', {'metadata': data.get('metadata', {})}, to=room_id, include_self=False)
    
    print(f"[Server] Client {sid[:8]} joined room {room_id[:8]}. Room is now OCCUPIED. "
          f"(creator={room['createdBy'][:8]}, joiner={sid[:8]})")
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
    
    leave_room(room_id)
    _untrack_user_room(sid, room_id)
    
    if room_id in rooms:
        _destroy_room(room_id, reason=f"leave by {sid[:8]}")


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
