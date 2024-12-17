import configparser
import json
import logging
import netrc
import os
import pprint
import requests
import requests.exceptions

# from http.client import HTTPConnection
# HTTPConnection.debuglevel = 1

logfmt = '%(levelname)s:%(funcName)s[%(lineno)d] %(message)s'
loglvl = logging.INFO
#loglvl = logging.DEBUG
#logging.basicConfig( level=loglvl, format=logfmt )

# requests_log = logging.getLogger("urllib3")
# requests_log.setLevel(loglvl)
# requests_log.propagate = True


resources = {} # module level resources


def get_session():
    key = 'session'
    if key not in resources:
        resources[key] = requests.Session()
    return resources[key]


# def get_config():
#     key = 'cfg'
#     if key not in resources:
#         envvar = 'JIRA_TOOLS_CONFIG'
#         default_fn = '~/.config/jira-tools/config.ini'
#         conf_file = os.getenv( envvar, default_fn )
#         # try:
#         #     conf_file = os.environ[ envvar ]
#         # except KeyError as e:
#         #     logging.error( f"Env var '{envvar}' must be set" )
#         #     raise SystemExit( 1 )
#         cfg = configparser.ConfigParser( allow_no_value=True )
#         cfg.optionxform = str
#         cfg.read( conf_file )
#         resources[key] = cfg
#     return resources[key]


# def get_server():
#     key = 'server'
#     config = get_config()
#     if key not in resources:
#         server = config['server']['server']
#         resources[key] = server
#     return resources[key]


def get_netrc():
    key = 'netrc'
    if key not in resources:
        n = netrc.netrc()
        # n = netrc.netrc('/root/netrcfile')
        server = get_server()
        (login, account, password) = n.authenticators( server )
        resources['login'] = login
        resources['account'] = account
        resources['password'] = password
        resources[key] = n
    return resources[key]


def get_login():
    key = 'login'
    if key not in resources:
        get_netrc()
    return resources[key]


def get_account():
    key = 'account'
    if key not in resources:
        get_netrc()
    return resources[key]


def get_password():
    key = 'password'
    if key not in resources:
        get_netrc()
    return resources[key]


def get_warnings():
    key = 'errs'
    if key not in resources:
        resources[key] = []
    return resources[key]


def warn( msg ):
    ''' Log an warning to the screen and,
        Also save it in an array for later retrieval of all warnings.
    '''
    key = 'errs'
    if key not in resources:
        resources[key] = []
    resources[key].append( msg )
    logging.warning( msg )


def get_errors():
    key = 'errs'
    if key not in resources:
        resources[key] = []
    return resources[key]


def err( msg ):
    ''' Log an error to the screen and,
        Also save it in an array for later retrieval of all errors.
    '''
    key = 'errs'
    if key not in resources:
        resources[key] = []
    resources[key].append( msg )
    logging.error( msg )


# def get_role_id( name ):
#     key = 'roles'
#     if key not in resources:
#         path = f'role'
#         r = api_get( path )
#         rawdata = r.json()
#         resources[key] =  { d['name']: d['id'] for d in rawdata }
#     return resources[key][name]


def api_go( method, path, version='latest', **kw ):
    url = f'https://{get_server()}/rest/api/{version}/{path}'
    logging.debug( f'{method} {path}, {pprint.pformat(kw)}' )
    s = get_session()
    # to use personal access token, must disable netrc function in requests
    s.trust_env = False
    token = get_account()
    s.headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        }
    r = s.request( method, url, **kw )
    logging.debug( f'RETURN CODE .. {r}' )
    # logging.debug( f'RETURN HEADERS .. {r.headers}' )
    r.raise_for_status()
    return r


def api_get( path, params=None ):
    return api_go( 'GET', path, params=params )


def api_delete( path, data=None ):
    kwargs = { 'timeout': 1800 }
    if data:
        kwargs.update ( { 'json': data } )
    return api_go( 'DELETE', path, **kwargs )


def api_post( path, data):
    return api_go( 'POST', path, json=data )


def api_put( path, data ):
    return api_go( 'PUT', path, json=data )



if __name__ == '__main__':
    raise UserWarning( 'Not directly runnable' )
