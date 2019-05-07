import string
import re
from tld import get_fld

WWW = re.compile('^www\.')


def parse_domain(domain):
    if not domain:
        return domain
    domain = domain.lower()  # force lowercase to use binary collate in the db
    # app ids come clean
    if domain.isdigit():
        return domain
    if domain.startswith('com.'):
        return domain.split('&', 1)[0]
    domain = ''.join(filter(string.printable.__contains__, domain))  # remove non printable characters
    domain_cleaned = WWW.subn('', domain.replace('"', '').strip().split('//', 1)[-1].split('/', 1)[0], count=1)[0]
    # return cleaned top level domain or discard
    try:
        return get_fld("http://" + domain_cleaned)
    except:
        return domain_cleaned
