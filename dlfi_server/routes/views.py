import json
import logging
from pathlib import Path
from flask import Blueprint, render_template, current_app, redirect, url_for, request, session

logger = logging.getLogger(__name__)

views_bp = Blueprint("views", __name__)


def get_vault_info(vault_path: Path) -> dict:
	"""Get info about a vault from its path."""
	config_path = vault_path / ".dlfi" / "config.json"
	encrypted = False
	if config_path.exists():
		try:
			with open(config_path, 'r', encoding='utf-8') as f:
				vault_config = json.load(f)
				encrypted = vault_config.get("encrypted", False)
		except:
			pass
	
	return {
		"name": vault_path.name,
		"path": str(vault_path.resolve()),
		"encrypted": encrypted
	}


@views_bp.route("/")
def home():
	"""Home page - vault selection."""
	config = current_app.config["DLFI_CONFIG"]
	default_dir = config.default_vaults_dir
	
	# Find vaults in default directory
	default_vaults = []
	if default_dir.exists():
		try:
			for item in default_dir.iterdir():
				if item.is_dir() and (item / ".dlfi").exists():
					default_vaults.append(get_vault_info(item))
		except PermissionError:
			pass
	
	default_vaults.sort(key=lambda x: x["name"].lower())
	
	# Get recent vaults (from other locations)
	recent_vaults = []
	for vault_info in config.get_recent_vaults():
		vault_path = Path(vault_info["path"])
		# Skip if it's in the default directory
		try:
			vault_path.relative_to(default_dir)
			# If we get here, it's inside default_dir, skip it
			continue
		except ValueError:
			# Not inside default_dir, include it
			recent_vaults.append(vault_info)
	
	return render_template(
		"home.html",
		default_vaults=default_vaults,
		recent_vaults=recent_vaults,
		default_dir=str(default_dir)
	)


@views_bp.route("/vault")
def vault_view():
	"""Main vault viewer."""
	dlfi = current_app.config.get("DLFI_INSTANCE")
	
	if dlfi is None:
		return redirect(url_for("views.home"))
	
	vault_name = Path(dlfi.root).name
	vault_path = str(dlfi.root)
	encrypted = dlfi.config.encrypted
	
	return render_template(
		"vault.html",
		vault_name=vault_name,
		vault_path=vault_path,
		encrypted=encrypted
	)


@views_bp.route("/close")
def close_vault():
	"""Close current vault and return to home."""
	dlfi = current_app.config.get("DLFI_INSTANCE")
	
	if dlfi is not None:
		try:
			dlfi.close()
		except:
			pass
		current_app.config["DLFI_INSTANCE"] = None
		current_app.config["DLFI_PASSWORD"] = None
	
	session.clear()
	return redirect(url_for("views.home"))