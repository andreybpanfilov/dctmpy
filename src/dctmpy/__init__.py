# Copyright (c) 2013 Andrey B. Panfilov <andrew@panfilov.tel>
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# See the file 'CHANGES' for a list of changes
#


import calendar
import locale
import platform
import re
import time

from dctmpy.exceptions import ParserException
from dctmpy.obj.typecache import TypeCache

LONG_LOCALES = {
    'Unknown': 0, 'German': 1, 'English_US': 2, 'English_UK': 3, 'Spanish_Modern': 4, 'Spanish_Castilian': 5,
    'Swedish': 6, 'Finnish': 7, 'French': 8, 'French_Canadian': 9, 'Icelandic': 10, 'Italian': 11, 'Dutch': 12,
    'Norwegian': 13, 'Portuguese': 15, 'Danish': 16, 'Japanese': 17, 'Korean': 18, 'Afar': 19, 'Abkhazian': 20,
    'Afrikaans': 21, 'Amharic': 22, 'Arabic': 23, 'Assamese': 24, 'Aymara': 25, 'Azerbaijani': 26, 'Bashkir': 27,
    'Byelorussian': 28, 'Bulgarian': 29, 'Bihari': 30, 'Bislama': 31, 'Bengali': 32, 'Tibetan': 33, 'Breton': 34,
    'Catalan': 35, 'Corsican': 36, 'Czech': 37, 'Welsh': 38, 'Bhutani': 39, 'Greek': 40, 'Esperanto': 41,
    'Estonian': 42, 'Basque': 43, 'Persian': 44, 'Fiji': 45, 'Faroese': 46, 'Frisian': 47, 'Irish': 48, 'Gaelic': 49,
    'Galician': 50, 'Guarani': 51, 'Gujarati': 52, 'Hausa': 53, 'Hebrew_he': 54, 'Hindi': 55, 'Croatian': 56,
    'Hungarian': 57, 'Armenian': 58, 'Interlingua': 59, 'Indonesian': 60, 'Interlingue': 61, 'Inupiak': 62,
    'Inuktitut': 63, 'Javanese': 64, 'Georgian': 65, 'Kazakh': 66, 'Greenlandic': 67, 'Cambodian': 68, 'Kannada': 69,
    'Kashmiri': 70, 'Kurdish': 71, 'Kirghiz': 72, 'Latin': 73, 'Lingala': 74, 'Laothian': 75, 'Lithuanian': 76,
    'Latvian': 77, 'Malagasy': 78, 'Maori': 79, 'Macedonian': 80, 'Malayalam': 81, 'Mongolian': 82, 'Moldavian': 83,
    'Marathi': 84, 'Malay': 85, 'Maltese': 86, 'Burmese': 87, 'Nauru': 88, 'Nepali': 89, 'Occitan': 90, 'Oromo': 91,
    'Oriya': 92, 'Punjabi': 93, 'Polish': 94, 'Pashto': 95, 'Quechua': 96, 'Rhaeto_Romance': 97, 'Kirundi': 98,
    'Romanian': 99, 'Russian': 100, 'Kinyarwanda': 101, 'Sanskrit': 102, 'Sindhi': 103, 'Sangho': 104,
    'Serbo_Croatian': 105, 'Sinhalese': 106, 'Slovak': 107, 'Slovenian': 108, 'Samoan': 109, 'Shona': 110,
    'Somali': 111, 'Albanian': 112, 'Serbian': 113, 'Siswati': 114, 'Sesotho': 115, 'Sundanese': 116, 'Swahili': 117,
    'Tamil': 118, 'Telugu': 119, 'Tajik': 120, 'Thai': 121, 'Tigrinya': 122, 'Turkmen': 123, 'Tagalog': 124,
    'Setswana': 125, 'Tonga': 126, 'Turkish': 127, 'Tsonga': 128, 'Tatar': 129, 'Twi': 130, 'Uighur': 131,
    'Ukrainian': 132, 'Urdu': 133, 'Uzbek': 134, 'Vietnamese': 135, 'Volapuk': 136, 'Wolof': 137, 'Xhosa': 138,
    'Yiddish': 139, 'Yoruba': 140, 'Zhuang': 141, 'Chinese': 142, 'Zulu': 143,
    'Hebrew': 207, 'Norwegian_Bokmal': 214, 'Norwegian_Nynorsk': 218,
}

SHORT_LOCALES = {
    'ne': LONG_LOCALES['Nepali'], 'tr': LONG_LOCALES['Turkish'], 'da': LONG_LOCALES['Danish'],
    'gl': LONG_LOCALES['Galician'], 'my': LONG_LOCALES['Burmese'], 'ug': LONG_LOCALES['Uighur'],
    'ro': LONG_LOCALES['Romanian'], 'tn': LONG_LOCALES['Setswana'], 'ta': LONG_LOCALES['Tamil'],
    'co': LONG_LOCALES['Corsican'], 'rw': LONG_LOCALES['Kinyarwanda'], 'br': LONG_LOCALES['Breton'],
    'cy': LONG_LOCALES['Welsh'], 'bo': LONG_LOCALES['Tibetan'], 'st': LONG_LOCALES['Sesotho'],
    'ko': LONG_LOCALES['Korean'], 'mo': LONG_LOCALES['Moldavian'], 'cs': LONG_LOCALES['Czech'],
    'ps': LONG_LOCALES['Pashto'], 'km': LONG_LOCALES['Cambodian'], 'af': LONG_LOCALES['Afrikaans'],
    'is': LONG_LOCALES['Icelandic'], 'qu': LONG_LOCALES['Quechua'], 'ti': LONG_LOCALES['Tigrinya'],
    'mt': LONG_LOCALES['Maltese'], 'ky': LONG_LOCALES['Kirghiz'], 'fr_CA': LONG_LOCALES['French_Canadian'],
    'la': LONG_LOCALES['Latin'], 'hy': LONG_LOCALES['Armenian'], 'ga': LONG_LOCALES['Irish'],
    'ms': LONG_LOCALES['Malay'], 'bh': LONG_LOCALES['Bihari'], 'ka': LONG_LOCALES['Georgian'],
    'oc': LONG_LOCALES['Occitan'], 'mi': LONG_LOCALES['Maori'], 'sv': LONG_LOCALES['Swedish'],
    'it': LONG_LOCALES['Italian'], 'hu': LONG_LOCALES['Hungarian'], 'fa': LONG_LOCALES['Persian'],
    'za': LONG_LOCALES['Zhuang'], 'na': LONG_LOCALES['Nauru'], 'pt': LONG_LOCALES['Portuguese'],
    'hi': LONG_LOCALES['Hindi'], 'jw': LONG_LOCALES['Javanese'], 'ks': LONG_LOCALES['Kashmiri'],
    'ba': LONG_LOCALES['Bashkir'], 'no': LONG_LOCALES['Norwegian'], 'lv': LONG_LOCALES['Latvian'],
    'ln': LONG_LOCALES['Lingala'], 'fr': LONG_LOCALES['French'], 'id': LONG_LOCALES['Indonesian'],
    'sr': LONG_LOCALES['Serbian'], 'si': LONG_LOCALES['Sinhalese'], 'vo': LONG_LOCALES['Volapuk'],
    'om': LONG_LOCALES['Oromo'], 'ab': LONG_LOCALES['Abkhazian'], 'fi': LONG_LOCALES['Finnish'],
    'fj': LONG_LOCALES['Fiji'], 'wo': LONG_LOCALES['Wolof'], 'sn': LONG_LOCALES['Shona'], 'sd': LONG_LOCALES['Sindhi'],
    'yi': LONG_LOCALES['Yiddish'], 'ha': LONG_LOCALES['Hausa'], 'pa': LONG_LOCALES['Punjabi'],
    'sl': LONG_LOCALES['Slovenian'], 'am': LONG_LOCALES['Amharic'], 'bi': LONG_LOCALES['Bislama'],
    'mr': LONG_LOCALES['Marathi'], 'rm': LONG_LOCALES['Rhaeto_Romance'], 'dz': LONG_LOCALES['Bhutani'],
    'kn': LONG_LOCALES['Kannada'], 'rn': LONG_LOCALES['Kirundi'], 'fy': LONG_LOCALES['Frisian'],
    'eo': LONG_LOCALES['Esperanto'], 'ik': LONG_LOCALES['Inupiak'], 'mn': LONG_LOCALES['Mongolian'],
    'gd': LONG_LOCALES['Gaelic'], 'as': LONG_LOCALES['Assamese'], 'mg': LONG_LOCALES['Malagasy'],
    'tk': LONG_LOCALES['Turkmen'], 'su': LONG_LOCALES['Sundanese'], 'ru': LONG_LOCALES['Russian'],
    'ia': LONG_LOCALES['Interlingua'], 'nb': LONG_LOCALES['Norwegian_Bokmal'], 'ku': LONG_LOCALES['Kurdish'],
    'vi': LONG_LOCALES['Vietnamese'], 'az': LONG_LOCALES['Azerbaijani'], 'lo': LONG_LOCALES['Laothian'],
    'sg': LONG_LOCALES['Sangho'], 'aa': LONG_LOCALES['Afar'], 'ml': LONG_LOCALES['Malayalam'],
    'ts': LONG_LOCALES['Tsonga'], 'en_GB': LONG_LOCALES['English_UK'], 'uz': LONG_LOCALES['Uzbek'],
    'kl': LONG_LOCALES['Greenlandic'], 'iu': LONG_LOCALES['Inuktitut'], 'yo': LONG_LOCALES['Yoruba'],
    'to': LONG_LOCALES['Tonga'], 'eu': LONG_LOCALES['Basque'], 'iw': LONG_LOCALES['Hebrew'],
    'bg': LONG_LOCALES['Bulgarian'], 'gu': LONG_LOCALES['Gujarati'], 'ca': LONG_LOCALES['Catalan'],
    'pl': LONG_LOCALES['Polish'], 'sq': LONG_LOCALES['Albanian'], 'ay': LONG_LOCALES['Aymara'],
    'sk': LONG_LOCALES['Slovak'], 'uk': LONG_LOCALES['Ukrainian'], 'es': LONG_LOCALES['Spanish_Modern'],
    'sw': LONG_LOCALES['Swahili'], 'tt': LONG_LOCALES['Tatar'], 'fo': LONG_LOCALES['Faroese'],
    'or': LONG_LOCALES['Oriya'], 'ss': LONG_LOCALES['Siswati'], 'sa': LONG_LOCALES['Sanskrit'],
    'sh': LONG_LOCALES['Serbo_Croatian'], 'xh': LONG_LOCALES['Xhosa'], 'th': LONG_LOCALES['Thai'],
    'ie': LONG_LOCALES['Interlingue'], 'et': LONG_LOCALES['Estonian'], 'so': LONG_LOCALES['Somali'],
    'tl': LONG_LOCALES['Tagalog'], 'mk': LONG_LOCALES['Macedonian'], 'en': LONG_LOCALES['English_US'],
    'lt': LONG_LOCALES['Lithuanian'], 'hr': LONG_LOCALES['Croatian'], 'gn': LONG_LOCALES['Guarani'],
    'de': LONG_LOCALES['German'], 'be': LONG_LOCALES['Byelorussian'], 'zu': LONG_LOCALES['Zulu'],
    'ur': LONG_LOCALES['Urdu'], 'tw': LONG_LOCALES['Twi'], 'nn': LONG_LOCALES['Norwegian_Nynorsk'],
    'bn': LONG_LOCALES['Bengali'], 'ja': LONG_LOCALES['Japanese'], 'tg': LONG_LOCALES['Tajik'],
    'te': LONG_LOCALES['Telugu'], 'he': LONG_LOCALES['Hebrew_he'], 'zh': LONG_LOCALES['Chinese'],
    'sm': LONG_LOCALES['Samoan'], 'nl': LONG_LOCALES['Dutch'], 'ar': LONG_LOCALES['Arabic'],
    'el': LONG_LOCALES['Greek'], 'kk': LONG_LOCALES['Kazakh'], }

CHARSETS = {
    'EUC-JP': 5, 'EUC-KR': 24, 'EUC-TW': 17, 'EUROSHIFT-JIS': 44, 'IBM037': 28, 'IBM273': 29, 'IBM280': 30,
    'IBM285': 31, 'IBM297': 32, 'IBM500': 33, 'IBM930': 20, 'IBM935': 21, 'IBM937': 23, 'ISO-10646-UCS-2': 6,
    'ISO-8859-1': 2, 'ISO_8859-2': 7, 'ISO-8859-3': 8, 'ISO-8859-4': 9, 'ISO-8859-5': 10, 'ISO-8859-6': 11,
    'ISO-8859-7': 12, 'ISO-8859-8': 13, 'ISO-8859-9': 14, 'ISO-8859-10': 15, 'ISO-8859-15': 18, 'JEF': 43, 'LATIN-1': 2,
    'MACINTOSH': 3, 'MS1250': 34, 'MS1251': 35, 'MS1252': 36, 'MS1253': 37, 'MS1254': 38, 'MS1255': 39, 'MS1256': 40,
    'MS1257': 41, 'MS1258': 42, 'MS1361': 26, 'MS874': 19, 'MS932': 27, 'MS936': 22, 'MS949': 24, 'MS950': 25,
    'SHIFT-JIS': 4, 'US-ASCII': 1, 'UTF-8': 16, }

CHARSETS_REVERSE = dict((v, k) for k, v in CHARSETS.items())

PLATFORMS = {
    'WINDOWS': 4096, 'UNIX': 8192, 'RESERVED_1': 0, 'RESERVED_2': 1, 'MS_WINDOWS': 4099, 'MACINTOSH': 16388,
    'SUNOS': 8197, 'SOLARIS': 8198, 'HP_UX': 8199, 'AIX': 8200, 'LINUX': 8201, }

TYPE_PATTERN = re.compile('^(BOOL|INT|STRING|ID|TIME|DOUBLE|UNDEFINED)$')
ATTRIBUTE_PATTERN = re.compile('^.+$')
REPEATING_PATTERN = re.compile('^(R|S)$')
BASE64_PATTERN = re.compile('^[0-9a-zA-Z+/?]+?$')
INTEGER_PATTERN = re.compile('^-?\d+$')
ENCODING_PATTERN = re.compile('^(A|H)$')
BOOLEAN_PATTERN = re.compile('^(T|F|1|0)$')

SINGLE = "S"
REPEATING = "R"

BOOL = "BOOL"
INT = "INT"
STRING = "STRING"
ID = "ID"
TIME = "TIME"
DOUBLE = "DOUBLE"
UNDEFINED = "UNDEFINED"

TYPES = {
    0: BOOL,
    1: INT,
    2: STRING,
    3: ID,
    4: TIME,
    5: DOUBLE,
    6: UNDEFINED,
}

NULL_ID = "0" * 16
EMPTY_STRING = ""

MONTHS = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
}

ENCODE = {
    0: 'A', 1: 'B', 2: 'C', 3: 'D', 4: 'E', 5: 'F', 6: 'G', 7: 'H',
    8: 'I', 9: 'J', 10: 'K', 11: 'L', 12: 'M', 13: 'N', 14: 'O', 15: 'P',
    16: 'Q', 17: 'R', 18: 'S', 19: 'T', 20: 'U', 21: 'V', 22: 'W', 23: 'X',
    24: 'Y', 25: 'Z', 26: 'a', 27: 'b', 28: 'c', 29: 'd', 30: 'e', 31: 'f',
    32: 'g', 33: 'h', 34: 'i', 35: 'j', 36: 'k', 37: 'l', 38: 'm', 39: 'n',
    40: 'o', 41: 'p', 42: 'q', 43: 'r', 44: 's', 45: 't', 46: 'u', 47: 'v',
    48: 'w', 49: 'x', 50: 'y', 51: 'z', 52: '0', 53: '1', 54: '2', 55: '3',
    56: '4', 57: '5', 58: '6', 59: '7', 60: '8', 61: '9', 62: '+', 63: '/',
}

DECODE = dict((v, k) for k, v in ENCODE.items())

RPC_GET_BLOCK = 1
RPC_GET_BLOCK1 = 2
RPC_GET_BLOCK2 = 3
RPC_GET_BLOCK3 = 4
RPC_GET_BLOCK4 = 5
RPC_GET_BLOCK5 = 6
RPC_DO_PUSH = 27
RPC_NEW_SESSION_BY_ADDR = 51
RPC_CLOSE_SESSION = 52
RPC_FETCH_TYPE = 53
RPC_APPLY = 54
RPC_MULTI_NEXT = 56
RPC_CLOSE_COLLECTION = 57
RPC_APPLY_FOR_LONG = 58
RPC_APPLY_FOR_BOOL = 59
RPC_APPLY_FOR_ID = 60
RPC_APPLY_FOR_STRING = 61
RPC_APPLY_FOR_OBJECT = 62
RPC_APPLY_FOR_TIME = 63
RPC_APPLY_FOR_DOUBLE = 64
RPC_GET_NEXT_PIECE = 65

CLIENT_VERSION_STRING = "6.0.0.1 python"

DM_CLIENT_CONNECT_PROTOCOL = 2
DM_CLIENT_SESSION_RECORD_HINT = -1
DM_CLIENT_SERIALIZATION_VERSION_HINT = 2

DM_CLIENT_USE_OBDATA = 1 << 0
DM_CLIENT_USE_NEW_RPC = 1 << 1
DM_CLIENT_IS_DMCL = 1 << 2
DM_CLIENT_TZ_COMPAT = 0 << 3

CLIENT_VERSION_ARRAY = [
    0, DM_CLIENT_CONNECT_PROTOCOL, DM_CLIENT_SESSION_RECORD_HINT,
    DM_CLIENT_SERIALIZATION_VERSION_HINT, 0, 0, 0, 0, 0,
    DM_CLIENT_USE_OBDATA + DM_CLIENT_USE_NEW_RPC + DM_CLIENT_IS_DMCL + DM_CLIENT_TZ_COMPAT
]

DEFAULT_BATCH_SIZE = 20

ISO8601_REGEXP = "^([0-9]){4}(-([0-9]){2}){2}T([0-9]{2}:){2}([0-9]){2}Z"

CHUNKS = {
    RPC_GET_BLOCK1: 256,
    RPC_GET_BLOCK2: 1024,
    RPC_GET_BLOCK3: 4096,
    RPC_GET_BLOCK4: 16384,
    RPC_GET_BLOCK: 16384,
    RPC_GET_BLOCK5: 63000,
    17023: 0
}

INT_2_BASE64 = {}
BASE64_2_INT = {}


def get_platform_id():
    (system, release, version) = platform.system_alias(platform.system(), platform.release(), platform.version())
    if re.match("windows", system, re.I) or re.match("windows", release, re.I):
        return PLATFORMS['MS_WINDOWS']
    if re.match("solaris", system, re.I) or re.match("solaris", release, re.I):
        return PLATFORMS['SOLARIS']
    if re.match("aix", system, re.I) or re.match("aix", release, re.I):
        return PLATFORMS['AIX']
    if re.match("hpux", system, re.I) or re.match("hpux", release, re.I):
        return PLATFORMS['HP_UX']
    if re.match("linux", system, re.I) or re.match("linux", release, re.I):
        return PLATFORMS['LINUX']
    return 0


def get_charset_id():
    (system, release, version) = platform.system_alias(platform.system(), platform.release(), platform.version())
    data = re.split("_|\.|@?", locale.setlocale(locale.LC_ALL, ''))
    data[2] = data[2].replace("_", "-").upper()
    data[2] = data[2].replace("UTF8", "UTF-8")
    if data[2] in CHARSETS:
        return CHARSETS[data[2]]
    if (re.match("windows", system, re.I)
        or re.match("windows", release, re.I)) and "MS" + data[2] in CHARSETS:
        return CHARSETS["MS" + data[2]]
    return 0


def get_locale_id():
    data = re.split("_|\.|@", locale.setlocale(locale.LC_ALL, ''))
    if data[0] + "_" + data[1] in SHORT_LOCALES:
        return SHORT_LOCALES[data[0] + "_" + data[1]]
    if data[0] + "_" + data[1] in LONG_LOCALES:
        return LONG_LOCALES[data[0] + "_" + data[1]]
    if data[0] in SHORT_LOCALES:
        return SHORT_LOCALES[data[0]]
    if data[0] in LONG_LOCALES:
        return LONG_LOCALES[data[0]]
    return LONG_LOCALES['Unknown']


def get_offset_in_seconds():
    t = time.time()
    return int(time.mktime(time.gmtime(t)) - time.mktime(time.localtime(t)))


def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [] + value
    return [value]


def is_empty(value):
    if value is None:
        return True
    return len(value) == 0


def parse_address(value):
    if is_empty(value):
        raise ParserException("Invalid address: %s" % value)
    if not value.startswith("INET_ADDR"):
        raise ParserException("Invalid address: %s" % value)
    chunks = value.split(" ")
    return chunks[4] + ":" + str(int(chunks[2], 16))


def parse_time(value):
    if is_empty(value) or "nulldate" == value:
        return None
    if re.match(ISO8601_REGEXP, value):
        return parse_utc_time(value)
    return parse_dctm_time(value)


def parse_utc_time(value):
    chunks = re.split("[-:TZ]", value)
    if len(chunks) != 7:
        raise ParserException("Invalid date: %s" % value)
    return calendar.timegm(
        [int(chunks[0]), int(chunks[1]), int(chunks[2]),
         int(chunks[3]), int(chunks[4]), int(chunks[5])])


def parse_dctm_time(value):
    chunks = re.split("[: ]", value)
    if len(chunks) != 6:
        raise ParserException("Invalid date: %s" % value)
    if not chunks[0] in MONTHS:
        raise ParserException("Invalid month: %s" % chunks[0])
    return time.mktime(
        [int(chunks[5]), MONTHS[chunks[0]], int(chunks[1]),
         int(chunks[2]), int(chunks[3]), int(chunks[4]),
         0, 0, -1])


def get_type_from_cache(attrName):
    return TypeCache().get(attrName)


def add_type_to_cache(typeObj):
    TypeCache().add(typeObj)


def _int_to_pseudo_base64(value):
    result = ""
    while value >= 0x40:
        result += ENCODE.get(value & 0x3f)
        value >>= 6
    result += ENCODE[value]
    return result


def int_to_pseudo_base64(value):
    if value not in INT_2_BASE64:
        INT_2_BASE64[value] = _int_to_pseudo_base64(value)
    return INT_2_BASE64[value]


def pseudo_base64_to_int(value):
    if value not in BASE64_2_INT:
        BASE64_2_INT[value] = _pseudo_base64_to_int(value)
    return BASE64_2_INT[value]


def _pseudo_base64_to_int(value):
    result = 0
    for c in list(value)[::-1]:
        if c not in DECODE:
            return None
        result = ((result << 6) + DECODE[c])
    return result


def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def get_current_time_mills():
    return int(round(time.time() * 1000))


def create_chunk(data, offset, rpc):
    length = CHUNKS[rpc]
    length = min(length, len(data) - offset)
    if length == 0:
        return bytearray(), len(data), True
    return data[offset:offset + length], offset + length, (offset + length) == len(data)
