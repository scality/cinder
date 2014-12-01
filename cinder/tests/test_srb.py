# Copyright (c) 2014 Scality
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Unit tests for the Scality Rest Block Volume Driver.
"""

import mock
from oslo.concurrency import processutils

from cinder import context
from cinder import exception
from cinder.openstack.common import log as logging
from cinder.openstack.common import units
from cinder import test
from cinder.volume import configuration as conf
from cinder.volume.drivers import srb

LOG = logging.getLogger(__name__)


class SRBRetryTestCase(test.TestCase):

    def __init__(self, *args, **kwargs):
        super(SRBRetryTestCase, self).__init__(*args, **kwargs)
        self.attempts = 0

    def setUp(self):
        super(SRBRetryTestCase, self).setUp()
        self.attempts = 0

    def test_retry_fail_by_return(self):
        expected_attempts = 2

        @srb.retry(times=expected_attempts, success=True)
        def _try_failing(self):
            self.attempts = self.attempts + 1
            return False

        ret = _try_failing(self)

        self.assertEqual(ret, False)
        self.assertEqual(self.attempts, expected_attempts)

    def test_retry_fail_by_exception(self):
        expected_attempts = 2
        ret = None

        @srb.retry(times=expected_attempts,
                   exceptions=(processutils.ProcessExecutionError))
        def _try_failing(self):
            self.attempts = self.attempts + 1
            raise processutils.ProcessExecutionError("Fail everytime")
            return True

        try:
            ret = _try_failing(self)
        except processutils.ProcessExecutionError:
            pass

        self.assertEqual(ret, None)
        self.assertEqual(self.attempts, expected_attempts)

    def test_retry_fail_and_succeed_mixed(self):

        @srb.retry(times=4, success=34, exceptions=(Exception))
        def _try_failing(self):
            attempted = self.attempts
            self.attempts = self.attempts + 1
            if attempted == 0:
                return False
            if attempted == 1:
                raise Exception("Second try shall except")
            if attempted == 2:
                return True
            return 34

        ret = _try_failing(self)

        self.assertEqual(ret, 34)
        self.assertEqual(self.attempts, 4)


class SRBDriverTestCase(test.TestCase):
    """Test case for the Scality Rest Block driver."""

    def __init__(self, *args, **kwargs):
        super(SRBDriverTestCase, self).__init__(*args, **kwargs)
        self._urls = []
        self._volumes = {
            "fake-old-volume": {
                "name": "fake-old-volume",
                "size": 4 * units.Gi,
                "vgs": {
                    "fake-old-volume": {
                        "lvs": {"vol1": 4 * units.Gi},
                        "snaps": ["snap1", "snap2", "snap3"],
                    },
                },
            },
            "volume-extend": {
                "name": "volume-extend",
                "size": 4 * units.Gi,
                "vgs": {
                    "volume-extend": {
                        "lvs": {"volume-extend-pool": 0.95 * 4 * units.Gi,
                                "volume-extend": 4 * units.Gi},
                        "snaps": [],
                    },
                },
            },
            "volume-SnapBase": {
                "name": "volume-SnapBase",
                "size": 4 * units.Gi,
                "vgs": {
                    "volume-SnapBase": {
                        "lvs": {"volume-SnapBase-pool": 0.95 * 4 * units.Gi,
                                "volume-SnapBase": 4 * units.Gi},
                        "snaps": ['snapshot-SnappedBase', 'snapshot-delSnap'],
                    },
                },
            },
        }

    def _fake_execute(self, *cmd, **kwargs):
        cmd_string = ', '.join(cmd)
        data = "\n"

        ##
        #  To test behavior, we need to stub part of the brick/local_dev/lvm
        #  functions too, because we want to check the state between calls,
        #  not only if the calls were done
        ##
        if '> /sys/class/srb/add_urls' in cmd_string:
            self._urls.append(cmd[2].split()[1])
        elif '> /sys/class/srb/create' in cmd_string:
            volname = cmd[2].split()[1]
            volsize = cmd[2].split()[2]
            self._volumes[volname] = {
                "name": volname,
                "size": int(volsize),
                "vgs": {
                },
            }
        elif '> /sys/class/srb/destroy' in cmd_string:
            volname = cmd[2].split()[1]
            del self._volumes[volname]
        elif '> /sys/class/srb/extend' in cmd_string:
            volname = cmd[2].split()[1]
            volsize = cmd[2].split()[2]
            self._volumes[volname]["size"] = int(volsize)
        elif '> /sys/class/srb/attach' in cmd_string:
            pass
        elif '> /sys/class/srb/detach' in cmd_string:
            pass
        elif 'env, LC_ALL=C, vgs, --noheadings, -o, name' in cmd_string:
            # vg exists
            data = "  fake-outer-vg\n"
            for vname in self._volumes:
                vol = self._volumes[vname]
                for vgname in vol['vgs']:
                    data += "  " + vgname + "\n"
        elif 'env, LC_ALL=C, lvs, --noheadings, --unit=g, '\
            '-o, size,data_percent, --separator, :, --nosuffix'\
                in cmd_string:
            # get thinpool free space
            groupname = cmd[10].split('/')[2]
            poolname = cmd[10].split('/')[3]
            for vname in self._volumes:
                vol = self._volumes[vname]
                for vgname in vol['vgs']:
                    if vgname != groupname:
                        continue
                    vg = vol['vgs'][vgname]
                    for lvname in vg['lvs']:
                        if poolname != lvname:
                            continue
                        lv_size = vg['lvs'][lvname]
                        data += "  %.2f:0.00\n" % (lv_size / units.Gi)
        elif 'env, LC_ALL=C, vgs, --version' in cmd_string:
            data = "  LVM version:     2.02.95(2) (2012-03-06)\n"
        elif ('env, LC_ALL=C, lvs, --noheadings, --unit=g, '
              '-o, vg_name,name,size, --nosuffix' in cmd_string):
            # get_all_volumes
            data = "  fake-outer-vg fake-1 1.00g\n"
            for vname in self._volumes:
                vol = self._volumes[vname]
                for vgname in vol['vgs']:
                    vg = vol['vgs'][vgname]
                    for lvname in vg['lvs']:
                        lv_size = vg['lvs'][lvname]
                        data += "  %s %s %.2fg\n" %\
                            (vgname, lvname, lv_size)
        elif ('env, LC_ALL=C, pvs, --noheadings, --unit=g, '
              '-o, vg_name,name,size,free, --separator, :, --nosuffix'
              in cmd_string):
            # get_all_phys_volumes
            data = "  fake-outer-vg:/dev/fake1:10.00:1.00\n"
            for vname in self._volumes:
                vol = self._volumes[vname]
                for vgname in vol['vgs']:
                    vg = vol['vgs'][vgname]
                    for lvname in vg['lvs']:
                        lv_size = vg['lvs'][lvname]
                        data += "  %s:/dev/srb/%s/device:%.2f:%.2f\n" %\
                            (vgname, vol['name'],
                             lv_size / units.Gi, lv_size / units.Gi)
        elif ('env, LC_ALL=C, vgs, --noheadings, --unit=g, '
              '-o, name,size,free,lv_count,uuid, --separator, :, --nosuffix'
              in cmd_string):
            search_vgname = None
            if len(cmd) == 11:
                search_vgname = cmd[10]
            # get_all_volume_groups
            if search_vgname is None:
                data = "  fake-outer-vg:10.00:10.00:0:"\
                       "kVxztV-dKpG-Rz7E-xtKY-jeju-QsYU-SLG6Z1\n"
            for vname in self._volumes:
                vol = self._volumes[vname]
                for vgname in vol['vgs']:
                    if search_vgname is None or search_vgname == vgname:
                        vg = vol['vgs'][vgname]
                        data += "  %s:%.2f:%.2f:%i:%s\n" %\
                            (vgname,
                             vol['size'] / units.Gi, vol['size'] / units.Gi,
                             len(vg['lvs']) + len(vg['snaps']), vgname)
        elif 'udevadm, settle, ' in cmd_string:
            pass
        elif 'vgcreate, ' in cmd_string:
            volname = "volume-%s" % (cmd[2].split('/')[2].split('-')[1])
            vgname = cmd[1]
            self._volumes[volname]['vgs'][vgname] = {
                "lvs": {},
                "snaps": []
            }
        elif 'vgremove, -f, ' in cmd_string:
            volname = cmd[2]
            del self._volumes[volname]['vgs'][volname]
        elif 'vgchange, -ay, ' in cmd_string:
            pass
        elif 'vgchange, -an, ' in cmd_string:
            pass
        elif 'lvcreate, -T, -L, ' in cmd_string:
            # thin pool creation
            vgname = cmd[4].split('/')[0]
            lvname = cmd[4].split('/')[1]
            if cmd[3][-1] == 'g':
                lv_size = int(float(cmd[3][0:-1]) * units.Gi)
            elif cmd[3][-1] == 'B':
                lv_size = int(cmd[3][0:-1])
            else:
                lv_size = int(cmd[3])
            self._volumes[vgname]['vgs'][vgname]['lvs'][lvname] = lv_size
        elif 'lvcreate, -T, -V, ' in cmd_string:
            # think-lv creation
            vgname = cmd[6].split('/')[0]
            poolname = cmd[6].split('/')[1]
            lvname = cmd[5]
            if poolname not in self._volumes[vgname]['vgs'][vgname]['lvs']:
                raise AssertionError('thin-lv creation attempted before '
                                     'thin-pool creation: %s'
                                     % cmd_string)
            if cmd[3][-1] == 'g':
                lv_size = int(float(cmd[3][0:-1]) * units.Gi)
            elif cmd[3][-1] == 'B':
                lv_size = int(cmd[3][0:-1])
            else:
                lv_size = int(cmd[3])
            self._volumes[vgname]['vgs'][vgname]['lvs'][lvname] = lv_size
        elif 'lvcreate, --name, ' in cmd_string:
            vgname = cmd[4].split('/')[0]
            lvname = cmd[4].split('/')[1]
            snapname = cmd[2]
            if lvname not in self._volumes[vgname]['vgs'][vgname]['lvs']:
                raise AssertionError('snap creation attempted on non-existant '
                                     'thin-lv: %s' % cmd_string)
            if snapname[1:] in self._volumes[vgname]['vgs'][vgname]['snaps']:
                raise AssertionError('snap creation attempted on existing '
                                     'snapshot: %s' % cmd_string)
            self._volumes[vgname]['vgs'][vgname]['snaps'].append(snapname[1:])
        elif 'lvchange, -a, y, --yes' in cmd_string:
            pass
        elif ('lvremove, --config, activation { retry_deactivation = 1}, -f, '
              in cmd_string):
            vgname = cmd[4].split('/')[0]
            lvname = cmd[4].split('/')[1]
            if lvname in self._volumes[vgname]['vgs'][vgname]['lvs']:
                del self._volumes[vgname]['vgs'][vgname]['lvs'][lvname]
            elif lvname in self._volumes[vgname]['vgs'][vgname]['snaps']:
                self._volumes[vgname]['vgs'][vgname]['snaps'].remove(lvname)
            else:
                raise AssertionError('Cannot delete inexistant lv or snap'
                                     'thin-lv: %s' % cmd_string)
        elif ('env, LC_ALL=C, lvdisplay, --noheading, -C, -o, '
              'Attr, ' in cmd_string):
            # lv has snapshot
            vgname = cmd[7].split('/')[0]
            lvname = cmd[7].split('/')[1]
            if lvname not in self._volumes[vgname]['vgs'][vgname]['lvs']:
                raise AssertionError('Cannot check snaps for inexistant lv'
                                     ': %s' % cmd_string)
            if len(self._volumes[vgname]['vgs'][vgname]['snaps']):
                data = '  owi-a-\n'
            else:
                data = '  wi-a-\n'
        elif 'lvextend, -L, ' in cmd_string:
            vgname = cmd[3].split('/')[0]
            lvname = cmd[3].split('/')[1]
            if cmd[2][-1] == 'g':
                size = int(float(cmd[2][0:-1]) * units.Gi)
            elif cmd[2][-1] == 'B':
                size = int(cmd[2][0:-1])
            else:
                size = int(cmd[2])
            if vgname not in self._volumes:
                raise AssertionError('Cannot extend inexistant volume'
                                     ': %s' % cmd_string)
            if lvname not in self._volumes[vgname]['vgs'][vgname]['lvs']:
                raise AssertionError('Cannot extend inexistant lv'
                                     ': %s' % cmd_string)
            self._volumes[vgname]['vgs'][vgname]['lvs'][lvname] = size
        elif 'pvresize, ' in cmd_string:
            pass
        else:
            raise AssertionError('unexpected command called: %s' % cmd_string)

        return (data, "")

    def _configure_driver(self):
        srb.CONF.srb_base_urls = "http://localhost/volumes"

    def setUp(self):
        super(SRBDriverTestCase, self).setUp()

        self.configuration = conf.Configuration(None)
        self._driver = srb.SRBDriver(configuration=self.configuration)
        # Stub processutils.execute for static methods
        self.stubs.Set(processutils, 'execute', self._fake_execute)
        self._driver.set_execute(self._fake_execute)
        self._configure_driver()

    def test_setup(self):
        """The url shall be added automatically"""
        self._driver.do_setup(None)
        self.assertEqual(self._urls[0], "http://localhost/volumes")
        self._driver.check_for_setup_error()

    def test_setup_no_config(self):
        """The driver shall not start without any url configured"""
        srb.CONF.srb_base_urls = None
        self.assertRaises(exception.VolumeBackendAPIException,
                          self._driver.do_setup, None)

    def test_volume_create(self):
        """The volume shall be added in the internal
            state through fake_execute
        """
        volume = {'name': 'volume-test', 'id': 'test', 'size': 4 * units.Gi}
        old_vols = self._volumes
        updates = self._driver.create_volume(volume)
        self.assertEqual(updates,
                         {'provider_location': volume['name']})
        new_vols = self._volumes
        old_vols['volume-test'] = {
            'name': 'volume-test',
            'size': 4 * units.Gi,
            'vgs': {
                'volume-test': {
                    'lvs': {'volume-test-pool': 0.95 * 4 * units.Gi,
                            'volume-test': 4 * units.Gi},
                    'snaps': [],
                },
            },
        }
        self.assertDictMatch(old_vols, new_vols)

    def test_volume_delete(self):
        vol = {'name': 'volume-delete', 'id': 'delete', 'size': units.Gi}

        old_vols = self._volumes
        self._volumes['volume-delete'] = {
            'name': 'volume-delete',
            'size': units.Gi,
            'vgs': {
                'volume-delete': {
                    'lvs': {'volume-delete-pool': 0.95 * units.Gi,
                            'volume-delete': units.Gi},
                    'snaps': [],
                },
            },
        }
        self._driver.delete_volume(vol)
        new_vols = self._volumes
        self.assertDictMatch(old_vols, new_vols)

    def test_volume_create_and_delete(self):
        volume = {'name': 'volume-autoDelete', 'id': 'autoDelete',
                  'size': 4 * units.Gi}
        old_vols = self._volumes
        updates = self._driver.create_volume(volume)
        self.assertEqual(updates,
                         {'provider_location': volume['name']})
        self._driver.delete_volume(volume)
        new_vols = self._volumes
        self.assertDictMatch(old_vols, new_vols)

    def test_volume_create_cloned(self):
        with mock.patch('cinder.volume.utils.copy_volume'):
            new = {'name': 'volume-cloned', 'size': 4 * units.Gi,
                   'id': 'cloned'}
            old = {'name': 'volume-old', 'size': 4 * units.Gi, 'id': 'old'}
            old_vols = self._volumes
            self._volumes['volume-old'] = {
                'name': 'volume-old',
                'size': 4 * units.Gi,
                'vgs': {
                    'volume-old': {
                        'name': 'volume-old',
                        'lvs': {'volume-old-pool': 0.95 * 4 * units.Gi,
                                'volume-old': 4 * units.Gi},
                        'snaps': [],
                    },
                },
            }
            self._driver.create_cloned_volume(new, old)
            new_vols = self._volumes
            old_vols['volume-cloned'] = {
                'name': 'volume-cloned',
                'size': 4 * units.Gi,
                'vgs': {
                    'volume-cloned': {
                        'name': 'volume-cloned',
                        'lvs': {'volume-cloned-pool': 0.95 * 4 * units.Gi,
                                'volume-cloned': 4 * units.Gi},
                        'snaps': [],
                    },
                },
            }
            self.assertDictMatch(old_vols, new_vols)

    def test_volume_create_from_snapshot(self):
        cp_vol_patch = mock.patch('cinder.volume.utils.copy_volume')
        lv_activ_patch = mock.patch('cinder.brick.local_dev.lvm.LVM.active_lv')

        with cp_vol_patch as cp_vol, lv_activ_patch as lv_activ:
            old_vols = self._volumes
            newvol = {"name": "volume-SnapClone", "id": "SnapClone",
                      "size": 4 * units.Gi}
            srcsnap = {"name": "snapshot-SnappedBase", "id": "SnappedBase",
                       "volume_id": "SnapBase", "volume_size": 4,
                       "volume_name": "volume-SnapBase"}

            self._driver.create_volume_from_snapshot(newvol, srcsnap)

            expected_lv_activ_calls = [
                mock.call(srcsnap['volume_name'] + "-pool"),
                mock.call(srcsnap['name'], True)
            ]
            lv_activ.assertEqual(lv_activ.call_args_list,
                                 expected_lv_activ_calls)
            cp_vol.assert_called_with(
                '/dev/mapper/volume--SnapBase-_snapshot--SnappedBase',
                '/dev/mapper/volume--SnapClone-volume--SnapClone',
                srcsnap['volume_size'] * units.Ki,
                self._driver.configuration.volume_dd_blocksize,
                execute=self._fake_execute)

            new_vols = self._volumes
            old_vols['volume-SnapClone'] = {
                "name": 'volume-SnapClone',
                "id": 'SnapClone',
                "size": 30,
                "vgs": {
                    "name": 'volume-SnapClone',
                    "lvs": {'volume-SnapClone-pool': 0.95 * 30 * units.Gi,
                            'volume-SnapClone': 30 * units.Gi},
                    "snaps": [],
                },
            }
            self.assertDictMatch(old_vols, new_vols)

    def test_volume_snapshot_create(self):
        old_vols = self._volumes
        snap = {"name": "snapshot-SnapBase1",
                "id": "SnapBase1",
                "volume_name": "volume-SnapBase",
                "volume_id": "SnapBase",
                "volume_size": 4}
        self._driver.create_snapshot(snap)
        new_vols = self._volumes
        old_vols['volume-SnapBase']["vgs"]['volume-SnapBase']["snaps"].\
            append('snapshot-SnapBase1')
        self.assertDictMatch(old_vols, new_vols)

    def test_volume_snapshot_delete(self):
        old_vols = self._volumes
        snap = {"name": "snapshot-delSnap",
                "id": "delSnap",
                "volume_name": "volume-SnapBase",
                "volume_id": "SnapBase",
                "volume_size": 4}
        self._driver.delete_snapshot(snap)
        new_vols = self._volumes
        old_vols['volume-SnapBase']["vgs"]['volume-SnapBase']["snaps"].\
            remove(snap['name'])
        self.assertDictMatch(old_vols, new_vols)
        self.assertEqual(
            set(old_vols['volume-SnapBase']['vgs']
                ['volume-SnapBase']['snaps']),
            set(new_vols['volume-SnapBase']['vgs']
                ['volume-SnapBase']['snaps']))

    def test_volume_copy_from_image(self):
        with (mock.patch('cinder.image.image_utils.fetch_to_volume_format'))\
                as fetch:
            vol = {'name': 'volume-SnapBase', 'id': 'SnapBase',
                   'size': 5 * units.Gi}
            self._driver.copy_image_to_volume(context,
                                              vol,
                                              'image_service',
                                              'image_id')
            fetch.assert_called_once_with(context,
                                          'image_service',
                                          'image_id',
                                          self._driver._mapper_path(vol),
                                          'qcow2',
                                          self._driver.
                                          configuration.volume_dd_blocksize,
                                          size=vol['size'])

    def test_volume_copy_to_image(self):
        with mock.patch('cinder.image.image_utils.upload_volume') as upload:
            vol = {'name': 'volume-SnapBase', 'id': 'SnapBase',
                   'size': 5 * units.Gi}
            self._driver.copy_volume_to_image(context,
                                              vol,
                                              'image_service',
                                              'image_meta')
            upload.assert_called_once_with(context,
                                           'image_service',
                                           'image_meta',
                                           self._driver._mapper_path(vol))

    def test_volume_extend(self):
        vol = {'name': 'volume-extend', 'id': 'extend', 'size': 4 * units.Gi}
        new_size = 5

        self._driver.extend_volume(vol, new_size)

        new_vols = self._volumes
        self.assertEqual(new_vols['volume-extend']['size'],
                         new_size * units.Gi)
