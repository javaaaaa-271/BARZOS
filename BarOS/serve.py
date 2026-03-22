import os

from waitress import serve

from app import app


if __name__ == "__main__":
    host = os.getenv("BAROS_HOST", "0.0.0.0")
    port = int(os.getenv("PORT", os.getenv("BAROS_PORT", "5000")))
    serve(app, host=host, port=port)
