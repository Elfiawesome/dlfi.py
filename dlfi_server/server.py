import logging
from pathlib import Path
from typing import Optional
from flask import Flask

from .config import ServerConfig

logger = logging.getLogger(__name__)


def create_app(config: Optional[ServerConfig] = None) -> Flask:
	"""Create and configure the Flask application."""
	if config is None:
		config = ServerConfig()
	
	# Determine paths
	server_dir = Path(__file__).parent
	template_dir = server_dir / "templates"
	static_dir = server_dir / "static"
	
	app = Flask(
		__name__,
		template_folder=str(template_dir),
		static_folder=str(static_dir),
		static_url_path="/static"
	)
	
	# Configure app
	app.config["SECRET_KEY"] = config.secret_key
	app.config["MAX_CONTENT_LENGTH"] = config.max_upload_size
	app.config["DLFI_CONFIG"] = config
	app.config["DLFI_INSTANCE"] = None  # Will hold the active DLFI instance
	app.config["DLFI_PASSWORD"] = None  # Will hold the password for encrypted vaults
	
	# Register blueprints
	from .routes.views import views_bp
	from .routes.api import api_bp
	
	app.register_blueprint(views_bp)
	app.register_blueprint(api_bp, url_prefix="/api")
	
	logger.info(f"DLFI Server initialized (templates: {template_dir}, static: {static_dir})")
	
	return app


def run_server(config: Optional[ServerConfig] = None):
	"""Run the DLFI web server."""
	if config is None:
		config = ServerConfig()
	
	app = create_app(config)
	
	logger.info(f"Starting DLFI Server on http://{config.host}:{config.port}")
	
	app.run(
		host=config.host,
		port=config.port,
		debug=config.debug,
		threaded=True
	)