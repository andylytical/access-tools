import json
import logging
import netrc
import pathlib
import pprint
import requests
from tabulate import tabulate, SEPARATING_LINE

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


def get_file( site ):
    if not site:
        raise UserWarning( 'missing site name' )
    key = f'{site}.json'
    if key not in resources:
        resources[key] = pathlib.Path( key )
    return resources[key]
def get_site_lookup_table():
    return {
        'aces': 'tamu',
        'aces.tamu.access-ci.org': 'tamu',
        'anvil': 'purdue',
        'anvil-gpu.purdue.access-ci.org': 'purdue',
        'anvil.purdue.access-ci.org': 'purdue',
        'bridges-2': 'psc',
        'bridges2-em.psc.access-ci.org': 'psc',
        'bridges2-gpu-ai.psc.access-ci.org': 'psc',
        'bridges2-gpu.psc.access-ci.org': 'psc',
        'bridges2-rm.psc.access-ci.org': 'psc',
        'darwin': 'udel',
        'darwin.udel.access-ci.org': 'udel',
        'delta': 'ncsa',
        'delta-cpu.ncsa.access-ci.org': 'ncsa',
        'delta-gpu.ncsa.access-ci.org': 'ncsa',
        'derecho': '<<< ??? >>>',
        'expanse': 'sdsc',
        'expanse-gpu.sdsc.access-ci.org': 'sdsc',
        'expanse.sdsc.access-ci.org': 'sdsc',
        'faster': 'tamu',
        'faster.tamu.access-ci.org': 'tamu',
        'jetstream2': 'indiana',
        'jetstream2.indiana.access-ci.org': 'indiana',
        'kyric': 'uky',
        'kyric.uky.access-ci.org': 'uky',
        'ookami': 'sbu',
        'ookami.sbu.access-ci.org': 'sbu',
        'stampede3': 'tacc',
        'stampede3.tacc.access-ci.org': 'tacc',
        }


def get_rp_lookup_table():
    return {
        'aces': 'aces',
        'aces.tamu.access-ci.org': 'aces',
        'anvil': 'anvil',
        'anvil-gpu.purdue.access-ci.org': 'anvil-gpu',
        'anvil.purdue.access-ci.org': 'anvil',
        'bridges-2': 'bridges-2',
        'bridges2-em.psc.access-ci.org': 'bridges2-em',
        'bridges2-gpu-ai.psc.access-ci.org': 'bridges2-gpu-ai',
        'bridges2-gpu.psc.access-ci.org': 'bridges2-gpu',
        'bridges2-rm.psc.access-ci.org': 'bridges2-rm',
        'darwin': 'darwin',
        'darwin.udel.access-ci.org': 'darwin',
        'delta': 'delta-cpu',
        'delta-cpu.ncsa.access-ci.org': 'delta-cpu',
        'delta-gpu.ncsa.access-ci.org': 'delta-gpu',
        'derecho': 'derecho',
        'expanse': 'expanse',
        'expanse-gpu.sdsc.access-ci.org': 'expanse-gpu',
        'expanse.sdsc.access-ci.org': 'expanse',
        'faster': 'faster',
        'faster.tamu.access-ci.org': 'faster',
        'jetstream2': 'jetstream2',
        'jetstream2.indiana.access-ci.org': 'jetstream2',
        'kyric': 'kyric',
        'kyric.uky.access-ci.org': 'kyric',
        'ookami': 'ookami',
        'ookami.sbu.access-ci.org': 'ookami',
        'stampede3': 'stampede3',
        'stampede3.tacc.access-ci.org': 'stampede3',
        }


def get_sites():
    key = 'all_sites'
    if key not in resources:
        resources[key] = sorted( set( list( get_site_lookup_table().values() ) ) )
    return resources[key]


# def get_RPs():
#     key = 'all_RPs'
#     if key not in resources:
#         resources[key] = list( get_rp_lookup_table().values() ).sorted()
#     return resources[key]


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


def grab_sds_raw_data():
    site = 'sds'
    outfile = get_file( site )
    if not outfile.exists():
        server = "ara-db.ccs.uky.edu"
        api_key = get_account( server=server )
        path = 'software=*,include=software_versions+rp_name'
        url = f"https://{server}/api=API_0/{api_key}/{path}"
        r = api_get( url=url )
        with outfile.open('w') as target:
            raw = r.json()
            json.dump( raw, target )


def grab_ipf_raw_data():
    site = 'ipf'
    outfile = get_file( site )
    if not outfile.exists():
        server = 'operations-api.access-ci.org'
        path = 'wh2/glue2/v1/software_full/?format=json'
        url = f"https://{server}/{path}"
        r = api_get( url=url )
        with outfile.open('w') as target:
            raw = r.json()['results']
            json.dump( raw, target )


def load_json( site ):
    infile = get_file( site )
    with infile.open() as src:
        data = json.load( src )
    return data






def rp2site( rp ):
    return get_site_lookup_table()[rp]


def rp2rp( rp ):
    return get_rp_lookup_table()[rp]




def process_ipf_data():
    data = load_json( site='ipf' )
    results = {}
    for record in data:
        rp_raw = record['ResourceID']
        site_name = rp2site( rp_raw )
        site = results.setdefault( site_name, {} )
        rp_name = rp2rp( rp_raw )
        rp = site.setdefault( rp_name, {} )
        app_name = record['AppName']
        rp.setdefault( app_name, 0 )
        rp[ app_name ] += 1
    return results


def process_sds_data():
    data = load_json( site='sds' )
    results = {}
    for record in data:
        app_name = record[ 'software_name' ]
        for version_string in record[ 'software_versions' ]:
            rp_raw, versions = version_string.split(': ')
            rp_name = rp2rp( rp_raw )
            site_name = rp2site( rp_raw )
            version_count = versions.count( ',' ) + 1
            site = results.setdefault( site_name, {} )
            rp = site.setdefault( rp_name, {} )
            rp[ app_name ] = version_count
    return results


def summarize( data ):
    totals = {}
    for site, s_data in data.items():
        # totals[site] = { 'rp_count':len(s_data), 'app_count':0, 'version_count':0 }
        totals[site] = { 'app_count':0, 'version_count':0 }
        for rp, rp_data in s_data.items():
            totals[site]['app_count'] += len( rp_data )
            totals[site][rp] = {'app_count': len( rp_data ), 'version_count':0 }
            for app, version_count in rp_data.items():
                totals[site]['version_count'] += version_count
                totals[site][rp]['version_count'] += version_count
    return totals


def tabularize( data1, name1, data2, name2 ):
    headers = [
        'site', 'resource',
        f'{name1}\napp_count', f'{name2}\napp_count',
        f'{name1}\nversion_count', f'{name2}\nversion_count'
        ]
    table = []
    for site in get_sites():
        app_count1 = ''
        version_count1 = ''
        if site in data1:
            app_count1 = data1[site].pop( 'app_count' )
            version_count1 = data1[site].pop( 'version_count' )
        app_count2 = ''
        version_count2 = ''
        if site in data2:
            app_count2 = data2[site].pop( 'app_count' )
            version_count2 = data2[site].pop( 'version_count' )
        totals = [
            site.upper(),
            'TOTALS',
            app_count1,
            app_count2,
            version_count1,
            version_count2,
        ]
        rp_list = mk_rp_list( data1.get(site,{}), data2.get(site,{}) )
        for rp in rp_list:
            app_count1 = ''
            version_count1 = ''
            app_count2 = ''
            version_count2 = ''
            if site in data1:
                if rp in data1[site]:
                    app_count1 = data1[site][rp]['app_count']
                    version_count1 = data1[site][rp]['version_count']
            if site in data2:
                if rp in data2[site]:
                    app_count2 = data2[site][rp]['app_count']
                    version_count2 = data2[site][rp]['version_count']
            row = [
                site,
                rp,
                app_count1,
                app_count2,
                version_count1,
                version_count2,
            ]
            table.append( row )
        table.append( totals )
        table.append( SEPARATING_LINE )
    print( tabulate( table, headers ) )


def mk_rp_list( d1, d2 ):
    ignorelist = [ 'app_count', 'version_count' ]
    list1 = [ x for x in d1.keys() if x not in ignorelist ]
    list2 = [ x for x in d2.keys() if x not in ignorelist ]
    return sorted( set( list1 + list2 ) )


def run():
    grab_ipf_raw_data()
    ipf_results = process_ipf_data()
    ipf_summary = summarize( ipf_results )

    grab_sds_raw_data()
    sds_results = process_sds_data()
    sds_summary = summarize( sds_results )

    tabularize( ipf_summary, 'IPF', sds_summary, 'SDS' )
    

if __name__ == "__main__":
    run()
