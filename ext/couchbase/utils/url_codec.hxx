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

} // namespace couchbase::utils::string_codec
