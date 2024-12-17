import json
import logging
import netrc
import pathlib
import pprint
import requests

logfmt = '%(levelname)s:%(funcName)s[%(lineno)d] %(message)s'
loglvl = logging.INFO

resources = {} # module level resources


def get_session():
    key = 'session'
    if key not in resources:
        resources[key] = requests.Session()
    return resources[key]


def get_netrc( server ):
    if not server:
        raise UserWarning( 'missing server name' )
    key = f"netrc-{server}"
    if key not in resources:
        n = netrc.netrc()
        (login, account, password) = n.authenticators( server )
        resources[f'{key}-login'] = login
        resources[f'{key}-account'] = account
        resources[f'{key}-password'] = password
        resources[key] = n
    return resources[key]


def get_account( server ):
    if not server:
        raise UserWarning( 'missing server name' )
    key = f"netrc-{server}-account"
    if key not in resources:
        get_netrc( server=server )
    return resources[key]


def api_go( method, url, **kw ):
    logging.debug( f'{method} {url}, {pprint.pformat(kw)}' )
    s = get_session()
    # to use personal access token, must disable netrc function in requests
    s.trust_env = False
    s.headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        }
    r = s.request( method, url, **kw )
    logging.debug( f'RETURN CODE .. {r}' )
    # logging.debug( f'RETURN HEADERS .. {r.headers}' )
    r.raise_for_status()
    return r


def api_get( url, params=None ):
    return api_go( method='GET', url=url, params=params )


def get_sds_raw_data():
    outfile = pathlib.Path( 'sds.json' )
    if not outfile.exists():
        server = "ara-db.ccs.uky.edu"
        api_key = get_account( server=server )
        # pprint.pprint( resources )
        # raise UserWarning( f"API-KEY='{api_key}'" )
        path = 'software=*,include=software_versions+rp_name'
        url = f"https://{server}/api=API_0/{api_key}/{path}"
        r = api_get( url=url )
        with outfile.open('w') as target:
            raw = r.json()
            json.dump( raw, target )
        # outfile.write_text( raw.dump() )


def get_ipf_raw_data():
    outfile = pathlib.Path( 'ipf.json' )
    if not outfile.exists():
        server = 'operations-api.access-ci.org'
        path = 'wh2/glue2/v1/software_full/?format=json'
        url = f"https://{server}/{path}"
        r = api_get( url=url )
        with outfile.open('w') as target:
            raw = r.json()['results']
            json.dump( raw, target )
        # outfile.write_text( raw.dump() )


def run():
    get_sds_raw_data()
    get_ipf_raw_data()

if __name__ == "__main__":
    run()
