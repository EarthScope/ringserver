# Ringserver authorization workflow

The authentication mechanism for ringserver is to configure a program that is
executed and receives the authentication details via environment variables:
* `AUTH_USERNAME` and `AUTH_PASSWORD` or
* `AUTH_JWTOKEN`

The program is expected to authenticate the user based on these details and
return the permissions (authorizations) via a JSON object on `stdout`.  If an
error is encountered an error message can be returned on `stderr` and will be
included in the server log.

## Example authentication and authorization program

The `authorizer.py` script in this directory is an example of a program to
perform these actions based on users, passwords, and their permissions in a
configuration file (`authconfig.toml`).

Requirements:
* Python 3.11+ (for `tomllib`)
* [PyJWT](https://pyjwt.readthedocs.io/) for JWT support — install with
  `pip install -r requirements.txt` or `pip install PyJWT`

This would be configured in a ringserver config file as follows:

```Shell
AuthCommand /path/to/authorizer.py --config /path/to/authconfig.toml
```

The example program uses the python interpreter identified with: `/usr/bin/env
python3`.  If another interpreter is needed the script can be changed, or the
command can be configured to specify the interpreter followed by the script,
e.g.:

```Shell
AuthCommand /path/to/venv/bin/python3 /path/to/authorizer.py --config /path/to/authconfig.toml
```

These auth command config parameter can also be specified via the environment
variable `RS_AUTH_COMMAND`, for example:

```Shell
RS_AUTH_COMMAND="/path/to/authorizer.py --config /path/to/authconfig.toml" ringserver -Rd ring -L 18000 -DL 16000
```

## Configuration file layout

The `authconfig.toml` file has four sections:

**`[default_permissions]`** — Fallback permissions applied when a user or JWT
subject has no explicit overrides.

| Key                 | Type   | Description                                |
|---------------------|--------|--------------------------------------------|
| `write_permission`  | bool   | Allow writing streams (default `false`)    |
| `trust_permission`  | bool   | Allow trusted details (default `false`)    |
| `allowed_streams`   | list   | Stream ID regex patterns allowed           |
| `forbidden_streams` | list   | Stream ID regex patterns denied            |

**`[jwt]`** — JWT token verification settings.

| Key                       | Type   | Description                                      |
|---------------------------|--------|--------------------------------------------------|
| `secret`                  | string | Shared secret for HMAC signature verification    |
| `algorithm`               | string | Signing algorithm, e.g. `"HS256"` (default)      |
| `allow_unknown_subjects`  | bool   | Accept any valid JWT, even if sub is not listed   |

**`[jwt.subjects.<name>]`** — Per-subject permission overrides, keyed by the
JWT `sub` claim value.  Each entry may contain `write_permission`,
`trust_permission`, `allowed_streams`, and `forbidden_streams`.  Missing keys
fall back to `[default_permissions]`.

**`[users.<name>]`** — Username/password entries.  Each entry must have a
`password` key and may contain the same permission keys as JWT subjects.

## Authentication and permission specification as JSON

The authentication and authorization program is expected to print, assuming
there was no program error, a JSON object like the following on `stdout`:

```JSON
{
  "username": "<USERNAME>",
  "authenticated": <boolean>,
  "write_permission": <boolean>,
  "trust_permission": <boolean>,
  "allowed_streams": ["streamregex1", "streamregex2"],
  "forbidden_streams": ["streamregex3", "streamregex4"]
}
```

For example:

```JSON
{
  "username": "limited",
  "authenticated": true,
  "write_permission": false,
  "trust_permission": false,
  "allowed_streams": [
    "^FDSN:IU_.*",
    "^FDSN:II_.*"
  ],
  "forbidden_streams": [
    "^FDSN:XX_.*"
  ]
}
```

If the user is not authenticated, the `authenticated` value will be `false` and
the server will send a message to the client that the authentication failed and
will disconnect the client.

## Debugging

The authorization program operates independently from ringserver and can be
tested directly from the command line.  Errors are printed to stderr; the JSON
result is printed to stdout.

Check the exit code: the program exits with `1` only for configuration errors
(missing config file, no credentials).  Authentication failures still exit `0`
and return `"authenticated": false` in the JSON output.

## Testing username and password authentication

Each example user from `authconfig.toml` can be tested by passing credentials
via environment variables:

```Shell
# Default read-only user
AUTH_USERNAME="read" AUTH_PASSWORD="pass" /path/to/authorizer.py --config /path/to/authconfig.toml

# Limited user with stream restrictions
AUTH_USERNAME="limited" AUTH_PASSWORD="pass" /path/to/authorizer.py --config /path/to/authconfig.toml

# Trusted user
AUTH_USERNAME="trusted" AUTH_PASSWORD="trustpass" /path/to/authorizer.py --config /path/to/authconfig.toml

# Write user
AUTH_USERNAME="write" AUTH_PASSWORD="writepass" /path/to/authorizer.py --config /path/to/authconfig.toml
```

A wrong password returns `"authenticated": false`:

```Shell
AUTH_USERNAME="read" AUTH_PASSWORD="wrong" /path/to/authorizer.py --config /path/to/authconfig.toml
```

```JSON
{
  "username": "read",
  "authenticated": false
}
```

## Testing JWT authentication

Generate a token for any subject defined in `[jwt.subjects]` using the same
secret configured in `authconfig.toml`:

```Shell
python3 -c "
import jwt
token = jwt.encode({'sub': 'write'}, 'change-me-use-a-long-random-secret', algorithm='HS256')
print(token)
"
```

Pass the token to the authorizer via `AUTH_JWTOKEN`:

```Shell
AUTH_JWTOKEN="<token>" /path/to/authorizer.py --config /path/to/authconfig.toml
```

Expected output for the `write` subject:

```JSON
{
  "username": "write",
  "authenticated": true,
  "write_permission": true,
  "trust_permission": false
}
```

An expired or tampered token will produce `"authenticated": false` and an
error message on stderr.  A subject not listed in `[jwt.subjects]` will also
fail unless `allow_unknown_subjects = true` is set in `[jwt]`.
