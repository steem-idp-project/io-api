"""
IOService
"""

import os

import psycopg2
from flask import Flask, jsonify, render_template, request
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

app = Flask(__name__)


# --- Database Connection ---
def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
        )
        return conn
    except psycopg2.OperationalError as e:
        app.logger.error("Database connection error: %s", e)
        raise ConnectionError("Could not connect to the database.") from e


# --- Error Handling ---
@app.errorhandler(Exception)
def handle_exception(e):
    """Generic error handler."""
    app.logger.error("An unexpected error occurred: %s", e, exc_info=True)
    if isinstance(e, psycopg2.Error):
        response = jsonify({"error": "Database error", "detail": str(e)})
        response.status_code = 500
        try:
            conn = get_db_connection()
            conn.rollback()
            conn.close()
        except psycopg2.Error as rollback_e:
            app.logger.error("Error during rollback: %s", rollback_e)
        return response
    if isinstance(e, ConnectionError):
        response = jsonify({"error": "Database connection failed", "detail": str(e)})
        response.status_code = 503
        return response
    if hasattr(e, "code") and isinstance(e.code, int) and 400 <= e.code < 600:
        response = jsonify({"error": e.name, "detail": e.description})
        response.status_code = e.code
        return response

    response = jsonify(
        {
            "error": "Internal Server Error",
            "detail": "An unexpected error occurred.",
        }
    )
    response.status_code = 500
    return response


# --- Index Route ---
@app.route("/", methods=["GET"])
def index():
    """
    Index page
    """
    try:
        conn = get_db_connection()
        conn.close()
        db_status = "Connected"
    except ConnectionError:
        db_status = "Connection Failed"
    return render_template("index.html", db_status=db_status)


# --- Users ---
@app.route("/users", methods=["POST"])
def create_user():
    """Creates a new user."""
    data = request.get_json()

    email = data["email"]
    passwd = data["passwd"]
    is_publisher = data.get("is_publisher", False)
    is_admin = data.get("is_admin", False)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            INSERT INTO Users (email, passwd, is_publisher, is_admin)
            VALUES (%s, %s, %s, %s) RETURNING uid, email, is_publisher, is_admin;
            """,
            (email, passwd, is_publisher, is_admin),
        )
        user = cur.fetchone()
        conn.commit()
        cur.execute(
            """
            INSERT INTO Wallets (uid, balance) VALUES (%s, %s);
            """,
            (user["uid"], 0),
        )
        conn.commit()
        return jsonify(user), 201
    except psycopg2.Error as e:
        conn.rollback()
        app.logger.error("Error creating user: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/users", methods=["GET"])
def get_users():
    """Retrieves all users."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT uid, email, is_publisher, is_admin FROM Users ORDER BY uid;"
        )
        users = cur.fetchall()
        return jsonify(users), 200
    except psycopg2.Error as e:
        app.logger.error("Error fetching users: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/users/<int:uid>", methods=["GET"])
def get_user(uid):
    """Retrieves a single user by uid."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT uid, email, is_publisher, is_admin FROM Users WHERE uid = %s;",
            (uid,),
        )
        user = cur.fetchone()
        if user:
            return jsonify(user), 200
        return jsonify({"error": "User not found"}), 404
    except psycopg2.Error as e:
        app.logger.error("Error fetching user %s: %s", uid, e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/users/<int:uid>", methods=["PUT"])
def update_user(uid):
    """Updates an existing user."""
    data = request.get_json()

    set_parts = []
    values = []
    valid_fields = [
        "email",
        "passwd",
        "is_publisher",
        "is_admin",
    ]
    for key, value in data.items():
        if key in valid_fields:
            set_parts.append(sql.SQL("{} = %s").format(sql.Identifier(key)))
            values.append(value)

    values.append(uid)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        query = sql.SQL(
            "UPDATE Users SET {} WHERE uid = %s RETURNING uid, email, is_publisher, is_admin;"
        ).format(sql.SQL(", ").join(set_parts))
        cur.execute(query, tuple(values))
        user = cur.fetchone()
        if user:
            conn.commit()
            return jsonify(user), 200
        conn.rollback()
        return jsonify({"error": "User not found"}), 404
    except psycopg2.Error as e:
        conn.rollback()
        app.logger.error("Error updating user %s: %s", uid, e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/users/<int:uid>", methods=["DELETE"])
def delete_user(uid):
    """Deletes a user."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM Users WHERE uid = %s RETURNING uid;", (uid,))
        deleted_user = cur.fetchone()
        if deleted_user:
            conn.commit()
            return jsonify({"message": f"User {uid} deleted successfully"}), 200
        conn.rollback()
        return jsonify({"error": "User not found"}), 404
    except psycopg2.Error as e:
        conn.rollback()
        app.logger.error("Error deleting user %s: %s", uid, e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


# --- Wallets ---
@app.route("/wallets/<int:uid>", methods=["GET"])
def get_wallet(uid):
    """Retrieves a wallet by user uid."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT uid, balance FROM Wallets WHERE uid = %s;", (uid,))
        wallet = cur.fetchone()
        if wallet:
            return jsonify(wallet), 200
        return jsonify({"error": "Wallet not found"}), 404
    except psycopg2.Error as e:
        app.logger.error("Error fetching wallet for user %s: %s", uid, e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/wallets/<int:uid>", methods=["PUT"])
def update_wallet_balance(uid):
    """Updates the balance of a wallet."""
    data = request.get_json()

    balance = data["balance"]

    balance = int(balance)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "UPDATE Wallets SET balance = %s WHERE uid = %s RETURNING uid, balance;",
            (balance, uid),
        )
        wallet = cur.fetchone()
        if wallet:
            conn.commit()
            return jsonify(wallet), 200
        conn.rollback()
        return jsonify({"error": "Wallet not found"}), 404
    except psycopg2.Error as e:
        conn.rollback()
        app.logger.error("Error updating wallet for user %s: %s", uid, e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


# --- Games ---
@app.route("/games", methods=["POST"])
def create_game():
    """Creates a new game."""
    data = request.get_json()

    name = data["name"]
    description = data.get("description", "")
    price = data["price"]
    uid = data["publisher"]
    status = data["status"]

    price = int(price)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            INSERT INTO games (name, description, price, publisher, status)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING gid, name, description, price, publisher, status;
            """,
            (name, description, price, uid, status),
        )
        game = cur.fetchone()
        conn.commit()
        return jsonify(game), 201
    except psycopg2.Error as e:
        conn.rollback()
        app.logger.error("Error creating game: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/games", methods=["GET"])
def get_games():
    """Retrieves all games."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            SELECT g.gid, g.name, g.description, g.price, g.publisher, u.email as publisher_email, g.status
            FROM games g
            JOIN Users u ON g.publisher = u.uid
            ORDER BY g.gid;
        """
        )
        games = cur.fetchall()
        return jsonify(games), 200
    except psycopg2.Error as e:
        app.logger.error("Error fetching games: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/games/<int:gid>", methods=["GET"])
def get_game(gid):
    """Retrieves a single game by gid."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            SELECT g.gid, g.name, g.description, g.price, g.publisher, u.email as publisher_email, g.status
            FROM games g
            JOIN Users u ON g.publisher = u.uid
            WHERE g.gid = %s;
        """,
            (gid,),
        )
        game = cur.fetchone()
        if game:
            return jsonify(game), 200
        return jsonify({"error": "Game not found"}), 404
    except psycopg2.Error as e:
        app.logger.error("Error fetching game %s: %s", gid, e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/games/<int:gid>", methods=["PUT"])
def update_game(gid):
    """Updates an existing game."""
    data = request.get_json()

    set_parts = []
    values = []
    valid_fields = ["name", "description", "price", "status"]
    for key, value in data.items():
        if key in valid_fields:
            if key == "price":
                price = int(value)
                values.append(price)
            else:
                values.append(value)
            set_parts.append(sql.SQL("{} = %s").format(sql.Identifier(key)))

    values.append(gid)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        query = sql.SQL(
            "UPDATE games SET {} WHERE gid = %s RETURNING gid, name, description, price, publisher, status;"
        ).format(sql.SQL(", ").join(set_parts))
        cur.execute(query, tuple(values))
        game = cur.fetchone()

        if game:
            conn.commit()
            return jsonify(game), 200
        conn.rollback()
        return jsonify({"error": "Game not found"}), 404
    except psycopg2.Error as e:
        conn.rollback()
        app.logger.error("Error updating game %s: %s", gid, e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/games/<int:gid>", methods=["DELETE"])
def delete_game(gid):
    """Deletes a game."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM games WHERE gid = %s RETURNING gid;", (gid,))
        game = cur.fetchone()
        if game:
            conn.commit()
            return jsonify({"message": f"Game {gid} deleted successfully"}), 200
        conn.rollback()
        return jsonify({"error": "Game not found"}), 404
    except psycopg2.Error as e:
        conn.rollback()
        app.logger.error("Error deleting game %s: %s", gid, e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


# --- Purchases ---
@app.route("/purchases", methods=["POST"])
def create_purchase():
    """Creates a new purchase record."""
    data = request.get_json()

    game_id = data["game_id"]
    user_id = data["user_id"]

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            INSERT INTO purchases (game_id, user_id)
            VALUES (%s, %s)
            RETURNING pid, game_id, user_id, date, hours_played;
            """,
            (game_id, user_id),
        )
        purchase = cur.fetchone()
        conn.commit()
        return jsonify(purchase), 201
    except psycopg2.Error as e:
        conn.rollback()
        app.logger.error("Error creating purchase: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/purchases", methods=["GET"])
def get_purchases():
    """Retrieves all purchase records."""
    user_id = request.args.get("user_id", type=int)
    game_id = request.args.get("game_id", type=int)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    base_query = """
        SELECT p.pid, p.game_id, g.name as game_name, p.user_id, u.email as user_email, p.date, p.hours_played
        FROM purchases p
        JOIN games g ON p.game_id = g.gid
        JOIN users u ON p.user_id = u.uid
    """
    conditions = []
    params = []

    if user_id is not None:
        conditions.append("p.user_id = %s")
        params.append(user_id)
    if game_id is not None:
        conditions.append("p.game_id = %s")
        params.append(game_id)

    if conditions:
        query = (
            base_query + " WHERE " + " AND ".join(conditions) + " ORDER BY p.date DESC;"
        )
    else:
        query = base_query + " ORDER BY p.date DESC;"

    try:
        cur.execute(query, tuple(params))
        purchases = cur.fetchall()
        return jsonify(purchases), 200
    except psycopg2.Error as e:
        app.logger.error("Error fetching purchases: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/purchases/<int:pid>", methods=["GET"])
def get_purchase(pid):
    """Retrieves a single purchase by pid."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            SELECT p.pid, p.game_id, g.name as game_name, p.user_id, u.email as user_email, p.date, p.hours_played
            FROM purchases p
            JOIN games g ON p.game_id = g.gid
            JOIN users u ON p.user_id = u.uid
            WHERE p.pid = %s;
        """,
            (pid,),
        )
        purchase = cur.fetchone()
        if purchase:
            return jsonify(purchase), 200
        return jsonify({"error": "Purchase not found"}), 404
    except psycopg2.Error as e:
        app.logger.error("Error fetching purchase %s: %s", pid, e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/purchases/<int:pid>", methods=["PUT"])
def update_purchase(pid):
    """Updates an existing purchase record."""
    data = request.get_json()

    hours_played = data["hours_played"]

    hours_played = int(hours_played)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            UPDATE purchases SET hours_played = %s WHERE pid = %s
            RETURNING pid, game_id, user_id, date, hours_played;
            """,
            (hours_played, pid),
        )
        purchase = cur.fetchone()
        if purchase:
            conn.commit()
            return jsonify(purchase), 200
        conn.rollback()
        return jsonify({"error": "Purchase not found"}), 404
    except psycopg2.Error as e:
        conn.rollback()
        app.logger.error("Error updating purchase %s: %s", pid, e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/purchases/<int:pid>", methods=["DELETE"])
def delete_purchase(pid):
    """Deletes a purchase record."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM purchases WHERE pid = %s RETURNING pid;", (pid,))
        purchase = cur.fetchone()
        if purchase:
            conn.commit()
            return jsonify({"message": f"Purchase {pid} deleted successfully"}), 200
        conn.rollback()
        return jsonify({"error": "Purchase not found"}), 404
    except psycopg2.Error as e:
        conn.rollback()
        app.logger.error("Error deleting purchase %s: %s", pid, e)
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()
