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

This would be configured in a ringserver config file as follows:

```Shell
AuthCommand /path/to/authorizer.py --config /path/to/autoconfig.toml
```

The example program uses the python interpreter identified with: `/usr/bin/env
python3`.  If another interpreter is needed the script can be changed, or the
command can be configured to specify the interpreter followed by the script,
e.g.:

```Shell
AuthCommand /path/to/venv/bin/python3 /path/to/authorizer.py --config /path/to/autoconfig.toml
```

These auth command config parameter can also be specified via the environment
variable `RS_AUTH_COMMAND`, for example:

```Shell
RS_AUTH_COMMAND="/pathå/to/authorizer.py --config /path/to/autoconfig.toml" ringserver -Rd ring -L 18000 -DL 16000
```

## Authentication and permission specification is JSON

The authentication and authorization program is expected to print, assuming
there was no program error, a JSON object like the following on `stdout`:

```JSON
{
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
  "name": "limited",
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

## Debugging an authorization program

The authorization program should operate independently from ringserver and is
then easy to debug.  For example, the following command can be used to run the
authorizer.py script with a username and password passed in environment
variables:

```Shell
AUTH_USERNAME="read" AUTH_PASSWORD="pass" /path/to/authorizer.py --config /path/to/autoconfig.toml
```

The program should print a JSON object similar to:

```JSON
{
  "name": "read",
  "authenticated": true,
  "write_permission": false,
  "trust_permission": false
}
```
