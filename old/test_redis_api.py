from flask import Flask, jsonify, request
import redis
import os

app = Flask(__name__)

# Redis connection
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_db = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)


@app.route("/get/<key>", methods=["GET"])
def get_values(key):
    try:
        values = redis_db.lrange(key, 0, -1)
        if not values:
            return jsonify({"error": "Key not found or list is empty"}), 404

        return_val = {}

        for value in enumerate(values):
            pass

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
