from flask import Flask, jsonify
import os

app = Flask(__name__)

# Injectée au build par Docker (voir Dockerfile / pipeline CI), correspond
# au SHA du commit Git à l'origine de cette image. Permet de vérifier
# visuellement, via /version, qu'un nouveau déploiement a bien eu lieu.
VERSION = os.environ.get("APP_VERSION", "dev")


@app.route("/")
def home():
    return jsonify(message="Hello from the CI/CD demo API!", version=VERSION)


@app.route("/version")
def version():
    return jsonify(version=VERSION)


@app.route("/healthz")
def healthz():
    return jsonify(status="ok")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
