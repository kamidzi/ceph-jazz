#!/usr/bin/env python
import subprocess
import itertools
from collections import namedtuple
from pprint import pprint
try:
    import simplejson as json
except ImportError:
    import json


PgId = namedtuple('PgId', ['osd_id', 'pg_num'])


def parse_pg_dump(input):
    pgs = json.loads(input)
    return pgs


def parse_ids(id_set):
    return map(lambda agg_id: PgId(*agg_id.split('.')), id_set)


def dump_stuck_pgs():
    cmd = 'ceph pg dump_stuck -fjson'
    return subprocess.check_output(cmd.split(' '))


def generate_pool_index():
    # {"poolnum":19,"poolname":".users"}]
    cmd = 'ceph osd lspools -fjson'
    out = subprocess.check_output(cmd.split(' '))
    pools = json.loads(out)
    return dict(map(lambda x: (x['poolnum'], x['poolname']), pools))


def translate(input, index={}):
    ret = {}
    for k, v in input.iteritems():
        try:
            i = int(k)
            ret[index[i]] = v
        except (ValueError, KeyError):
            print('Skipped %s - missing from index' % k)
    return ret


if __name__ == '__main__':
    pg_out = dump_stuck_pgs()
    pgs = parse_pg_dump(pg_out)
    ids = parse_ids(map(lambda x: x['pgid'], pgs))

    def by_osd_id(pg_id):
        return pg_id.osd_id

    osd_map = {}
    for osd_id, pgs in itertools.groupby(ids, key=by_osd_id):
        val = osd_map.get(osd_id)
        osd_map[osd_id] = [] if val is None else val
        osd_map[osd_id].extend(pgs)

    index = generate_pool_index()
    pprint(translate(osd_map, index))
