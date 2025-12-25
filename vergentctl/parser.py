
class ParseError(Exception):
    pass


SAFE_CONTEXT = {
    "true": True,
    "false": False,
    "null": None,
}

MAX_KEY_BYTES = 512


def validate_key(key: str):
    if not isinstance(key, str):
        raise ParseError("Key must be a string")

    encoded = key.encode("utf-8")
    if len(encoded) > MAX_KEY_BYTES:
        raise ParseError(f"Key too long (max {MAX_KEY_BYTES} bytes)")

    return key


def parse_value(raw: str):
    try:
        value = eval(raw, {}, SAFE_CONTEXT)
    except Exception:
        raise ParseError(f"Invalid syntax: {raw}")

    if not isinstance(value, (dict, list, int, float, bool, str, type(None))):
        raise ParseError(f"Invalid syntax: {raw}")

    return value


def split_key_value(args: str):
    """
    Split a raw argument string into (key, value_str).

    This function extracts:
      - the first token as the key, supporting both single and double quotes
      - the remaining part of the string as the raw value (unparsed)

    Rules:
      - Quoted keys are supported: "my key", 'my key'
      - Escaping inside quotes is supported: \" or \'
      - The value is returned exactly as-is (no parsing here)
      - If quotes are not properly closed, an error is raised
      - If no key is found, an error is raised

    This parser is intentionally minimal and deterministic:
      - It only parses the key
      - It does NOT tokenize the value
      - It does NOT interpret JSON or Python literals
    """
    s = args.lstrip()
    if not s:
        raise ParseError("Missing key")

    buf = []
    i = 0
    n = len(s)
    in_single = False
    in_double = False
    escape = False

    # Parse only the key token
    while i < n:
        ch = s[i]

        if escape:
            buf.append(ch)
            escape = False
            i += 1
            continue

        if ch == "\\":
            escape = True
            i += 1
            continue

        if ch == "'" and not in_double:
            in_single = not in_single
            i += 1
            continue

        if ch == '"' and not in_single:
            in_double = not in_double
            i += 1
            continue

        # End of key: whitespace outside quotes
        if ch.isspace() and not in_single and not in_double:
            i += 1
            break

        buf.append(ch)
        i += 1

    if in_single or in_double:
        raise ParseError("Unterminated quotes in key")

    key = "".join(buf)
    rest = s[i:].lstrip()

    if not key:
        raise ParseError("Key cannot be empty")

    return key, rest
