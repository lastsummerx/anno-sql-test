import re
from dataclasses import dataclass
from enum import Enum

_WORD_CHARS = re.compile(r'[\w.]+')


class TokenKind(Enum):
    STRING = 'string'
    STAR = 'star'
    COLON = 'colon'
    WORD = 'word'
    OTHER = 'other'


@dataclass
class Token:
    kind: TokenKind
    value: str
    start: int
    end: int


def skip_string(s: str, i: int) -> int:
    if i >= len(s) or s[i] not in ("'", '"'):
        return i
    quote = s[i]
    j = i + 1
    while j < len(s):
        if s[j] == '\\':
            j += 2
            continue
        if s[j] == quote:
            return j + 1
        j += 1
    return j


def tokenize(s: str) -> list[Token]:
    tokens = []
    i = 0
    while i < len(s):
        if s[i] in ("'", '"'):
            end = skip_string(s, i)
            tokens.append(Token(TokenKind.STRING, s[i:end], i, end))
            i = end
        elif s[i] == '*':
            tokens.append(Token(TokenKind.STAR, '*', i, i + 1))
            i += 1
        elif s[i] == ':':
            tokens.append(Token(TokenKind.COLON, ':', i, i + 1))
            i += 1
        elif m := _WORD_CHARS.match(s, i):
            tokens.append(Token(TokenKind.WORD, m.group(), i, m.end()))
            i = m.end()
        else:
            tokens.append(Token(TokenKind.OTHER, s[i], i, i + 1))
            i += 1
    return tokens
