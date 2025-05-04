#!/usr/bin/env python3

# A simple authentication and authorization system for a server
# using a TOML configuration file.
#
# Credentials are read from environment variables:
# AUTH_USERNAME and AUTH_PASSWORD for username/password authentication
# AUTH_JWTOKEN for JWT token authentication
#
# Note: JWT authentication is not implemented in this version, but is
# stubbed out for use by those who want to implement it for their system.
#
# The program reads a TOML configuration file containing user credentials
# and permissions. It checks the provided credentials against the
# configuration file and returns a JSON object with the authentication
# result and authorization details.  See the companion authconfig.toml
# file for details.
#
# On successful authentication, it returns a JSON object with the
# authorization details in the following format:
# {
#   "subject": "<SUBJECT>",   // Either username (USERNAME) or sub (from JWT)
#   "authenticated": true,    // if user is authentication was successful
#   "write_permission": true, // if user can write streams
#   "trust_permission": true, // if user can see connection and server details
#   "allowed_streams": [      // list of streams allowed for this user
#     "<STREAM_ID_PATTERN1>",
#     "<STREAM_ID_PATTERN2>",
#     ...],
#   "forbidden_streams": [    // list of streams forbidden for this user
#     "<STREAM_ID_PATTERN1>",
#     "<STREAM_ID_PATTERN2>",
#     ...]
# }
#
# If authentication fails, it returns a JSON object with the
# authentication result in the following format:
# {
#   "subject": "<SUBJECT>",   // Either username (USERNAME) or sub (from JWT)
#   "authenticated": false,   // if user is authentication was successful
# }

import os
import sys
import json
import tomllib
import argparse
from typing import Dict, Optional


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


def authenticate_with_credentials(username: str, password: str, config: Dict) -> tuple[Optional[str], bool]:
    """Authenticate using username and password."""
    users = config.get("users", {})

    if username in users and users[username].get("password") == password:
        return (username, True)
    return (username, False)


def authenticate_with_jwt(token: str, config: Dict) -> tuple[Optional[str], bool]:
    """Authenticate using JWT token."""

    print("Error: JWT authentication not supported", file=sys.stderr)
    return (None, False)


def get_authorization_details(name: str, config: Dict, result: Dict) -> None:
    """Get authorization details for authenticated user."""
    users = config.get("users", {})
    default_permissions = config.get("default_permissions", {})

    # Get the user's data
    user_data = users.get(name, {})

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
    if jwt_token:
        # JWT token takes precedence if provided
        (name, authenticated) = authenticate_with_jwt(jwt_token, config)
    elif username and password:
        # Fall back to username/password if both are provided
        (name, authenticated) = authenticate_with_credentials(username, password, config)
    else:
        print("Authentication failed: No valid credentials provided in environment variables.", file=sys.stderr)
        print("Set either AUTH_JWTOKEN or both AUTH_USERNAME and AUTH_PASSWORD.", file=sys.stderr)
        sys.exit(1)

    result = {
        "name": name,
        "authenticated": authenticated
    }

    if authenticated:
        get_authorization_details(name, config, result)

    # Output as JSON
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
