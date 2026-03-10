#!/usr/bin/env python3

# A simple authentication and authorization system for a server
# using a TOML configuration file.  Requires Python 3.11+ (for tomllib).
# JWT support additionally requires the PyJWT package (pip install PyJWT).
#
# Credentials are read from environment variables:
#   AUTH_USERNAME and AUTH_PASSWORD  for username/password authentication
#   AUTH_JWTOKEN                    for JWT token authentication
#
# The program reads a TOML configuration file containing user credentials
# and permissions.  For username/password, it checks against [users].
# For JWT, it verifies the token signature using the secret and algorithm
# configured in [jwt], then extracts the "sub" claim and looks up
# permissions in [jwt.subjects].  See authconfig.toml for details.
#
# On successful authentication, it returns a JSON object:
# {
#   "username": "<USERNAME>",     // username or JWT sub claim
#   "authenticated": true,        // authentication succeeded
#   "write_permission": true,     // user can write streams
#   "trust_permission": true,     // user can see connection/server details
#   "allowed_streams": [          // stream patterns the user may access
#     "<STREAM_ID_PATTERN>", ...
#   ],
#   "forbidden_streams": [        // stream patterns denied to the user
#     "<STREAM_ID_PATTERN>", ...
#   ]
# }
#
# If authentication fails:
# {
#   "username": "<USERNAME>",     // username or JWT sub claim (may be null)
#   "authenticated": false
# }

import os
import sys
import json
import tomllib
import argparse
from typing import Dict, Optional

try:
    import jwt as pyjwt
except ImportError:
    pyjwt = None


def read_toml_config(file_path: str) -> Dict:
    """Read and parse the TOML configuration file."""
    try:
        with open(file_path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{file_path}' not found.", file=sys.stderr)
        sys.exit(1)
    except tomllib.TOMLDecodeError as e:
        print(f"Error: Invalid TOML format in '{file_path}': {e}", file=sys.stderr)
        sys.exit(1)


def authenticate_with_userpass(username: str, password: str, config: Dict) -> tuple[Optional[str], bool]:
    """Authenticate using username and password."""
    users = config.get("users", {})

    if username in users and users[username].get("password") == password:
        return (username, True)
    return (username, False)


def authenticate_with_jwt(token: str, config: Dict) -> tuple[Optional[str], bool]:
    """Authenticate using JWT token.

    Decodes and verifies the token using the secret and algorithm from
    config [jwt], then extracts the 'sub' claim. The subject must appear
    in [jwt.subjects] (or default_permissions apply if allow_unknown_subjects
    is true).
    """
    if pyjwt is None:
        print("Error: PyJWT library not installed (pip install PyJWT)", file=sys.stderr)
        return (None, False)

    jwt_config = config.get("jwt", {})
    secret = jwt_config.get("secret")
    algorithm = jwt_config.get("algorithm", "HS256")

    if not secret:
        print("Error: JWT secret not configured in [jwt] section", file=sys.stderr)
        return (None, False)

    try:
        payload = pyjwt.decode(token, secret, algorithms=[algorithm])
    except pyjwt.ExpiredSignatureError:
        print("Error: JWT token has expired", file=sys.stderr)
        return (None, False)
    except pyjwt.InvalidTokenError as e:
        print(f"Error: JWT token invalid: {e}", file=sys.stderr)
        return (None, False)

    sub = payload.get("sub")
    if not sub:
        print("Error: JWT token missing 'sub' claim", file=sys.stderr)
        return (None, False)

    known_subjects = jwt_config.get("subjects", {})
    allow_unknown = jwt_config.get("allow_unknown_subjects", False)

    if sub not in known_subjects and not allow_unknown:
        print(f"Error: JWT subject '{sub}' not authorized", file=sys.stderr)
        return (sub, False)

    return (sub, True)


def get_authorization_details(name: str, auth_method: str, config: Dict, result: Dict) -> None:
    """Get authorization details for an authenticated identity.

    auth_method is "userpass" or "jwt", controlling which config section
    is consulted for permissions.
    """
    default_permissions = config.get("default_permissions", {})

    if auth_method == "jwt":
        user_data = config.get("jwt", {}).get("subjects", {}).get(name, {})
    else:
        user_data = config.get("users", {}).get(name, {})

    # Extract permissions directly from user data (not from a nested "permissions" object)
    # Merge default permissions with user-specific ones
    result["write_permission"] = user_data.get("write_permission", default_permissions.get("write_permission", False))
    result["trust_permission"] = user_data.get("trust_permission", default_permissions.get("trust_permission", False))

    # Handle allowed streams
    if "allowed_streams" in user_data:
        result["allowed_streams"] = user_data["allowed_streams"]
    elif "allowed_streams" in default_permissions:
        result["allowed_streams"] = default_permissions["allowed_streams"]

    # Handle forbidden streams
    if "forbidden_streams" in user_data:
        result["forbidden_streams"] = user_data["forbidden_streams"]
    elif "forbidden_streams" in default_permissions:
        result["forbidden_streams"] = default_permissions["forbidden_streams"]


def main():
    parser = argparse.ArgumentParser(description="Authentication and authorization system")
    parser.add_argument("--config", required=True, help="Path to TOML configuration file")
    args = parser.parse_args()

    # Read configuration
    config = read_toml_config(args.config)

    # Get authentication credentials from environment variables
    username = os.environ.get("AUTH_USERNAME")
    password = os.environ.get("AUTH_PASSWORD")
    jwt_token = os.environ.get("AUTH_JWTOKEN")

    # Determine authentication method
    name = None
    authenticated = False
    auth_method = None
    if jwt_token:
        auth_method = "jwt"
        (name, authenticated) = authenticate_with_jwt(jwt_token, config)
    elif username and password:
        auth_method = "userpass"
        (name, authenticated) = authenticate_with_userpass(username, password, config)
    else:
        print("Authentication failed: No valid credentials provided in environment variables.", file=sys.stderr)
        print("Set either AUTH_JWTOKEN or both AUTH_USERNAME and AUTH_PASSWORD.", file=sys.stderr)
        sys.exit(1)

    result = {
        "username": name,
        "authenticated": authenticated
    }

    if authenticated:
        get_authorization_details(name, auth_method, config, result)

    # Output as JSON
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
