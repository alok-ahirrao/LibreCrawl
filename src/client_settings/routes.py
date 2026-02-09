from flask import Blueprint, jsonify, request, session
from src.auth_db import get_user_by_id
from src.client_settings.db import get_client_module_access, toggle_module_access, toggle_module_access_bulk

client_settings_bp = Blueprint('client_settings', __name__, url_prefix='/api/client-settings')

def is_admin():
    """Check if current user is admin."""
    if 'user_id' not in session:
        return False
    # Trust the session tier which handles LOCAL_MODE overrides
    return session.get('tier') == 'admin'

@client_settings_bp.route('/<client_id>/modules', methods=['GET'])
def get_client_modules(client_id):
    """
    Get module access for a specific client.
    Security: Admin only (for now, or self if we decide to let clients see their own config explicitly here)
    """
    import sys
    print(f"DEBUG: Session keys: {list(session.keys())}", file=sys.stderr)
    print(f"DEBUG: Session tier: {session.get('tier')}", file=sys.stderr)
    print(f"DEBUG: is_admin(): {is_admin()}", file=sys.stderr)
    print(f"DEBUG: user_id: {session.get('user_id')} vs client_id: {client_id}", file=sys.stderr)

    # Verify permission (Admin or Self)
    if not is_admin() and session.get('user_id') != client_id:
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403
    
    access = get_client_module_access(client_id)
    return jsonify({
        'success': True,
        'client_id': client_id,
        'access': access
    })

@client_settings_bp.route('/<client_id>/modules/toggle', methods=['POST'])
def toggle_module(client_id):
    """
    Toggle visibility for a module.
    Body: { module_slug: string, is_visible: boolean }
    Security: Admin only.
    """
    if not is_admin():
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    data = request.json
    module_slug = data.get('module_slug')
    is_visible = data.get('is_visible')

    if not module_slug or is_visible is None:
        return jsonify({'success': False, 'error': 'Missing items'}), 400

    success = toggle_module_access(client_id, module_slug, is_visible)
    
    if success:
        return jsonify({'success': True})
    else:
        return jsonify({'success': False, 'error': 'Database error'}), 500

@client_settings_bp.route('/<client_id>/modules/bulk', methods=['POST'])
def toggle_module_bulk(client_id):
    """
    Bulk toggle visibility for modules.
    Body: { updates: [{ module_slug: string, is_visible: boolean }, ...] }
    Security: Admin only.
    """
    if not is_admin():
        return jsonify({'success': False, 'error': 'Unauthorized'}), 403

    data = request.json
    updates = data.get('updates')

    if not updates or not isinstance(updates, list):
        return jsonify({'success': False, 'error': 'Missing or invalid updates list'}), 400

    success = toggle_module_access_bulk(client_id, updates)
    
    if success:
        return jsonify({'success': True})
    else:
        return jsonify({'success': False, 'error': 'Database error'}), 500
