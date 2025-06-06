from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
import sqlite3


app = Flask(__name__)
CORS(app)

def init_db():
    conn = sqlite3.connect('NextGenFitness.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                (ID INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT, 
                email Text Unique,
                password TEXT,
                role INTEGER)''')
    conn.commit()
    conn.close()

@app.route('/signup', methods=['POST'])
def signup():
    data = request.get_json()
    username = data.get('username')
    email = data.get('email')
    password = generate_password_hash(data.get('password'))
    role = int(data.get('role', 1))

    conn = sqlite3.connect('NextGenFitness.db')
    c = conn.cursor()

    # Check if username exists
    c.execute("SELECT * FROM User WHERE username = ?", (username,))
    if c.fetchone():
        conn.close()
        return jsonify({'error': 'Username already exists'}), 409

    # Check if email exists
    c.execute("SELECT * FROM User WHERE email = ?", (email,))
    if c.fetchone():
        conn.close()
        return jsonify({'error': 'Email already registered'}), 409

    # Generate new user_id
    c.execute("SELECT user_id FROM User ORDER BY user_id DESC LIMIT 1")
    last = c.fetchone()
    if last and last[0]:
        last_num = int(last[0][1:])  # Remove 'U' and convert to int
        new_num = last_num + 1
    else:
        new_num = 1
    new_user_id = f"U{new_num:03d}"

    # Insert user
    c.execute("INSERT INTO User (user_id, username, email, password, role) VALUES (?, ?, ?, ?, ?)",
              (new_user_id, username, email, password, role))
    conn.commit()
    conn.close()

    return jsonify({'message': 'User registered successfully', 'user_id': new_user_id}), 201

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    conn = sqlite3.connect('NextGenFitness.db')
    c = conn.cursor()
    c.execute("SELECT password FROM User WHERE username = ?", (username,))
    row = c.fetchone()
    conn.close()

    if row and check_password_hash(row[0], password):
        return jsonify({'message': 'Login successful'}), 200
    else:
        return jsonify({'error': 'Invalid username or password'}), 401
    
@app.route('/forgot-password', methods=['POST'])
def forgot_password():
    data = request.get_json()
    email = data.get('email')

    conn = sqlite3.connect('NextGenFitness.db')
    c = conn.cursor()
    c.execute("SELECT * FROM User WHERE email = ?", (email,))
    user = c.fetchone()
    conn.close()

    if user:
        return jsonify({'message': 'Email found'}), 200
    else:
        return jsonify({'error': 'Email not found'}), 404
    
@app.route('/reset-password', methods=['POST'])
def reset_password():
    data = request.get_json()
    email = data.get('email')
    new_password = data.get('new_password')

    if not email or not new_password:
        return jsonify({'error': 'Email and new password are required'}), 400

    hashed_password = generate_password_hash(new_password)

    conn = sqlite3.connect('NextGenFitness.db')
    c = conn.cursor()
    c.execute("UPDATE users SET password = ? WHERE email = ?", (hashed_password, email))
    conn.commit()
    conn.close()

    return jsonify({'message': 'Password has been successfully updated'}), 200



if __name__ == '__main__':
    init_db()
    app.run(debug=True)
