from flask import Flask, request
from flask_socketio import SocketIO, emit, join_room, leave_room
import uuid
import time

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# rooms: room_id -> { 'offer': sdp, 'status': 'waiting'|'occupied', 'createdBy': sid, 'metadata': {} }
rooms = {}

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

@socketio.on('get_ice_servers')
def handle_get_ice_servers(data):
    print(f"ICE servers requested by {request.sid}")
    return ICE_SERVERS

@socketio.on('connect')
def handle_connect():
    print(f"Client connected: {request.sid}")

@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    print(f"Client disconnected: {sid}")
    to_delete = []
    for room_id, room in rooms.items():
        if room['createdBy'] == sid:
            print(f"Ending session in room {room_id} because creator {sid} disconnected")
            emit('session_ended', room_id, to=room_id)
            to_delete.append(room_id)
    
    for rid in to_delete:
        del rooms[rid]

@socketio.on('find_room')
def handle_find_room(data):
    my_metadata = data.get('metadata', {})
    print(f"User {request.sid} searching for room with metadata: {my_metadata}")
    
    for room_id, room in rooms.items():
        if room['status'] == 'waiting' and room['createdBy'] != request.sid:
            room_meta = room.get('metadata', {})
            
            # Basic validation
            if my_metadata.get('targetGender') != 'Any Gender':
                if room_meta.get('myGender') != my_metadata.get('targetGender'):
                    continue
            
            if room_meta.get('targetGender') != 'Any Gender':
                if my_metadata.get('myGender') != room_meta.get('targetGender'):
                    continue
            
            print(f"Matching {request.sid} with room {room_id}")
            return {'room_id': room_id}
            
    print(f"No available room found for {request.sid}")
    return {'room_id': None}

@socketio.on('create_room')
def handle_create_room(data):
    room_id = str(uuid.uuid4())
    rooms[room_id] = {
        'id': room_id,
        'offer': data['offer'],
        'status': 'waiting',
        'createdBy': request.sid,
        'metadata': data.get('metadata', {}),
        'createdAt': time.time()
    }
    join_room(room_id)
    print(f"Room created by {request.sid}: {room_id}")
    return {'room_id': room_id}

@socketio.on('join_room')
def handle_join_room(data):
    room_id = data['room_id']
    print(f"Client {request.sid} attempting to join room: {room_id}")
    if room_id in rooms and rooms[room_id]['status'] == 'waiting':
        rooms[room_id]['status'] = 'occupied'
        join_room(room_id)
        # Notify the creator that someone joined
        emit('peer_joined', {'metadata': data.get('metadata', {})}, to=room_id, include_self=False)
        print(f"Client {request.sid} successfully joined room: {room_id}")
        return {'success': True, 'offer': rooms[room_id]['offer'], 'metadata': rooms[room_id]['metadata']}
    
    print(f"Join room {room_id} failed for {request.sid} (room occupied or doesn't exist)")
    return {'success': False}

@socketio.on('send_answer')
def handle_send_answer(data):
    room_id = data['room_id']
    print(f"Answer received from {request.sid} for room {room_id}")
    emit('answer', {'answer': data['answer']}, to=room_id, include_self=False)

@socketio.on('send_candidate')
def handle_send_candidate(data):
    room_id = data['room_id']
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
    if room_id:
        print(f"Client {request.sid} leaving room {room_id}")
        leave_room(room_id)
        emit('session_ended', room_id, to=room_id, include_self=False)
        if room_id in rooms:
            del rooms[room_id]

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
    return "OK", 200

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
