#pragma once

namespace couchbase::utils::string_codec
{
template<typename Ti, typename To>
bool
url_decode(Ti first, Ti last, To out, size_t& nout)
{
    for (; first != last && *first != '\0'; ++first) {
        if (*first == '%') {
            char nextbuf[3] = { 0 };
            size_t jj = 0;
            first++;
            nextbuf[0] = *first;
            for (; first != last && jj < 2; ++jj) {
                nextbuf[jj] = *first;
                if (jj != 1) {
                    first++;
                }
            }
            if (jj != 2) {
                return false;
            }

            unsigned octet = 0;
            if (sscanf(nextbuf, "%2X", &octet) != 1) {
                return false;
            }

            *out = static_cast<char>(octet);
        } else {
            *out = *first;
        }

        out++;
        nout++;
    }
    return true;
}

inline bool
url_decode(const char* input, char* output)
{
    const char* endp = input + strlen(input);
    size_t nout = 0;
    if (url_decode(input, endp, output, nout)) {
        output[nout] = '\0';
        return true;
    }
    return false;
}

inline bool
url_decode(char* in_out)
{
    return url_decode(in_out, in_out);
}

inline bool
url_decode(std::string& s)
{
    size_t n = 0;
    if (url_decode(s.begin(), s.end(), s.begin(), n)) {
        s.resize(n);
        return true;
    }
    return false;
}

std::string
url_decode(const std::string& src)
{
    std::string dst{};
    size_t n = 0;
    if (url_decode(src.begin(), src.end(), dst.begin(), n)) {
        dst.resize(n);
    }
    return dst;
}

namespace priv
{
inline bool
is_legal_uri_char(char c)
{
    auto uc = static_cast<unsigned char>(c);
    if ((isalpha(uc) != 0) || (isdigit(uc) != 0)) {
        return true;
    }
    switch (uc) {
        case '-':
        case '_':
        case '.':
        case '~':
        case '!':
        case '*':
        case '\'':
        case '(':
        case ')':
        case ';':
        case ':':
        case '@':
        case '&':
        case '=':
        case '+':
        case '$':
        case ',':
        case '/':
        case '?':
        case '#':
        case '[':
        case ']':
            return true;
        default:
            break;
    }
    return false;
}

template<typename T>
inline bool
is_already_escape(T first, T last)
{
    first++; // ignore '%'
    size_t jj;
    for (jj = 0; first != last && jj < 2; ++jj, ++first) {
        if (!isxdigit(*first)) {
            return false;
        }
    }
    return jj == 2;
}
} // namespace priv

template<typename Ti, typename To>
bool
url_encode(Ti first, Ti last, To& o, bool check_encoded = true)
{
    // If re-encoding detection is enabled, this flag indicates not to
    // re-encode
    bool skip_encoding = false;

    for (; first != last; ++first) {
        if (!skip_encoding && check_encoded) {
            if (*first == '%') {
                skip_encoding = priv::is_already_escape(first, last);
            } else if (*first == '+') {
                skip_encoding = true;
            }
        }
        if (skip_encoding || priv::is_legal_uri_char(*first)) {
            if (skip_encoding && *first != '%' && !priv::is_legal_uri_char(*first)) {
                return false;
            }

            o.insert(o.end(), first, first + 1);
        } else {
            unsigned int c = static_cast<unsigned char>(*first);
            size_t numbytes = 0;

            if ((c & 0x80) == 0) { /* ASCII character */
                numbytes = 1;
            } else if ((c & 0xE0) == 0xC0) { /* 110x xxxx */
                numbytes = 2;
            } else if ((c & 0xF0) == 0xE0) { /* 1110 xxxx */
                numbytes = 3;
            } else if ((c & 0xF8) == 0xF0) { /* 1111 0xxx */
                numbytes = 4;
            } else {
                return false;
            }

            do {
                char buf[4];
                sprintf(buf, "%%%02X", static_cast<unsigned char>(*first));
                o.insert(o.end(), &buf[0], &buf[0] + 3);
            } while (--numbytes && ++first != last);
        }
    }
    return true;
}

template<typename Tin, typename Tout>
bool
url_encode(const Tin& in, Tout& out)
{
    return url_encode(in.begin(), in.end(), out);
}

std::string
url_encode(const std::string& src)
{
    std::string dst{};
    url_encode(src.begin(), src.end(), dst);
    return dst;
}

/* See: https://url.spec.whatwg.org/#urlencoded-serializing: */
/*
 * 0x2A
 * 0x2D
 * 0x2E
 * 0x30 to 0x39
 * 0x41 to 0x5A
 * 0x5F
 * 0x61 to 0x7A
 *  Append a code point whose value is byte to output.
 * Otherwise
 *  Append byte, percent encoded, to output.
 */
template<typename Ti, typename To>
void
form_encode(Ti first, Ti last, To& out)
{
    for (; first != last; ++first) {
        auto c = static_cast<unsigned char>(*first);
        if (isalnum(c)) {
            out.insert(out.end(), first, first + 1);
            continue;
        }
        if (c == ' ') {
            char tmp = '+';
            out.insert(out.end(), &tmp, &tmp + 1);
        } else if ((c == 0x2A || c == 0x2D || c == 0x2E) || (c >= 0x30 && c <= 0x39) || (c >= 0x41 && c <= 0x5A) || (c == 0x5F) ||
                   (c >= 0x60 && c <= 0x7A)) {
            out.insert(out.end(), static_cast<char>(c));
        } else {
            char buf[3] = { 0 };
            out.insert(out.end(), '%');
            sprintf(buf, "%02X", c);
            out.insert(out.end(), &buf[0], &buf[0] + 2);
        }
    }
}

std::string
form_encode(const std::string& src)
{
    std::string dst;
    form_encode(src.begin(), src.end(), dst);
    return dst;
}

namespace v2
{
enum class encoding {
    encode_path,
    encode_path_segment,
    encode_host,
    encode_zone,
    encode_user_password,
    encode_query_component,
    encode_fragment,
};

bool
should_escape(char c, encoding mode)
{
    // §2.3 Unreserved characters (alphanum)
    if (('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')) {
        return false;
    }

    if (mode == encoding::encode_host || mode == encoding::encode_zone) {
        // §3.2.2 Host allows
        //	sub-delims = "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="
        // as part of reg-name.
        // We add : because we include :port as part of host.
        // We add [ ] because we include [ipv6]:port as part of host.
        // We add < > because they're the only characters left that we could possibly allow, and Parse will reject them if we escape them
        // (because hosts can't use %-encoding for ASCII bytes).
        switch (c) {
            case '!':
            case '$':
            case '&':
            case '\'':
            case '(':
            case ')':
            case '*':
            case '+':
            case ',':
            case ';':
            case '=':
            case ':':
            case '[':
            case ']':
            case '<':
            case '>':
            case '"':
                return false;

            default:
                break;
        }
    }

    switch (c) {
        case '-':
        case '_':
        case '.':
        case '~':
            // §2.3 Unreserved characters (mark)
            return false;

        case '$':
        case '&':
        case '+':
        case ',':
        case '/':
        case ':':
        case ';':
        case '=':
        case '?':
        case '@':
            // §2.2 Reserved characters (reserved)
            // Different sections of the URL allow a few of
            // the reserved characters to appear unescaped.
            switch (mode) {
                case encoding::encode_path: // §3.3
                    // The RFC allows : @ & = + $ but saves / ; , for assigning meaning to individual path segments. This package only
                    // manipulates the path as a whole, so we allow those last three as well. That leaves only ? to escape.
                    return c == '?';

                case encoding::encode_path_segment: // §3.3
                    // The RFC allows : @ & = + $ but saves / ; , for assigning meaning to individual path segments.
                    return c == '/' || c == ';' || c == ',' || c == '?';

                case encoding::encode_user_password: // §3.2.1
                    // The RFC allows ';', ':', '&', '=', '+', '$', and ',' in userinfo, so we must escape only '@', '/', and '?'. The
                    // parsing of userinfo treats ':' as special so we must escape that too.
                    return c == '@' || c == '/' || c == '?' || c == ':';

                case encoding::encode_query_component: // §3.4
                    // The RFC reserves (so we must escape) everything.
                    return true;

                case encoding::encode_fragment: // §4.1
                    // The RFC text is silent but the grammar allows everything, so escape nothing.
                    return false;

                default:
                    break;
            }

        default:
            break;
    }

    if (mode == encoding::encode_fragment) {
        // RFC 3986 §2.2 allows not escaping sub-delims. A subset of sub-delims are included in reserved from RFC 2396 §2.2. The remaining
        // sub-delims do not need to be escaped. To minimize potential breakage, we apply two restrictions: (1) we always escape sub-delims
        // outside of the fragment, and (2) we always escape single quote to avoid breaking callers that had previously assumed that single
        // quotes would be escaped. See issue #19917.
        switch (c) {
            case '!':
            case '(':
            case ')':
            case '*':
                return false;

            default:
                break;
        }
    }

    // Everything else must be escaped.
    return true;
}

constexpr auto upper_hex = "0123456789ABCDEF";

std::string
escape(const std::string& s, encoding mode)
{
    std::size_t space_count{ 0 };
    std::size_t hex_count{ 0 };

    for (const auto& c : s) {
        if (should_escape(c, mode)) {
            if (c == ' ' && mode == encoding::encode_query_component) {
                ++space_count;
            } else {
                ++hex_count;
            }
        }
    }

    if (space_count == 0 && hex_count == 0) {
        return s;
    }

    std::size_t required = s.size() + 2 * hex_count;
    std::string t;
    t.resize(required);

    if (hex_count == 0) {
        for (std::size_t i = 0; i < s.size(); ++i) {
            if (s[i] == ' ') {
                t[i] = '+';
            } else {
                t[i] = s[i];
            }
        }
        return t;
    }

    std::size_t j{ 0 };
    for (const auto& c : s) {
        if (c == ' ' && mode == encoding::encode_query_component) {
            t[j++] = '+';
        } else if (should_escape(c, mode)) {
            t[j++] = '%';
            t[j++] = upper_hex[(c >> 4U) & 0x0f];
            t[j++] = upper_hex[c & 0x0f];
        } else {
            t[j++] = c;
        }
    }
    return t;
}

/**
 * Escapes the string so it can be safely placed inside a URL query.
 *
 * @param s
 * @return
 */
std::string
query_escape(const std::string& s)
{
    return escape(s, encoding::encode_query_component);
}

/**
 * Escapes the string so it can be safely placed inside a URL path segment, replacing special characters (including /) with %XX sequences as
 * needed.
 */
std::string
path_escape(const std::string& s)
{
    return escape(s, encoding::encode_path);
}

std::string
form_encode(const std::map<std::string, std::string>& values)
{
    std::stringstream ss;
    bool first{ true };
    for (const auto& [key, value] : values) {
        if (first) {
            first = false;
        } else {
            ss << '&';
        }
        ss << query_escape(key) << '=' << query_escape(value);
    }
    return ss.str();
}
} // namespace v2

} // namespace couchbase::utils::string_codec
