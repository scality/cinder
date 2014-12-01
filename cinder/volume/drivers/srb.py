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
Scality's Rest Block Volume Driver.

It is a Kernel Block Driver relying on a REST Protocol to store the data on
a Scality storage platform.
"""

from contextlib import nested
import six
import time

from oslo.config import cfg

from cinder.brick.local_dev import lvm
from cinder import exception
from cinder.i18n import _LI, _LE, _LW
from cinder.image import image_utils
from cinder.openstack.common import excutils
from cinder.openstack.common.lockutils import synchronized
from cinder.openstack.common import log as logging
from cinder.openstack.common import processutils as putils
from cinder.openstack.common import units
from cinder import utils
from cinder.volume import driver
from cinder.volume import utils as volutils


LOG = logging.getLogger(__name__)

srb_opts = [
    cfg.StrOpt('srb_base_urls',
               default=None,
               help='Comma-separated list of REST servers to connect to'),
]

CONF = cfg.CONF
CONF.register_opts(srb_opts)


def retry(times=2, wait_before=False,
          base_wait=1, increase='increment', success=None,
          exceptions=()):
    def decorator(func):
        def wrapper(*args, **kwargs):
            ret = None
            attempts_left = times
            wtime = base_wait

            while attempts_left != 0:
                if attempts_left != times:
                    LOG.warning("Retrying failed call: retry %i"
                                % (times - attempts_left))

                excepted = False
                attempts_left = attempts_left - 1
                try:
                    LOG.debug("Trying attempt #%i" % (times - attempts_left))
                    ret = func(*args, **kwargs)
                except exceptions:
                    excepted = True
                    if attempts_left == 0:
                        raise

                # Allow both case to be considered as success:
                # - No ret value expected, No exception raised
                # - Ret value expected, success identified
                if (success is None and excepted is False)\
                        or (success is not None and ret == success):
                    break

                if wait_before and attempts_left != 0:
                    time.sleep(wtime)
                    if increase == 'increment':
                        wtime = wtime + base_wait
                    elif increase == 'double':
                        wtime = wtime * 2

            if success is not None:
                return ret
            return

        return wrapper

    return decorator


class SRBDriver(driver.VolumeDriver):
    """REST Block Driver's cinder driver.

    Creates and manages volume files on a REST-based storage service
    for hypervisors to use as block devices through a native linux
    Loadable Kernel Module.
    """

    VERSION = '0.1.0'

    OVER_ALLOC_RATIO = 2

    class TempSnapshot(object):
        def __init__(self, drv, volume, src_vref):
            self._driver = drv
            self._dst = volume
            self._src = src_vref
            self._snap = {'volume_name': self._src['name'],
                          'volume_id': self._src['id'],
                          'volume_size': self._src['size'],
                          'name': 'snapshot-clone-%s' % self._dst['id'],
                          'id': 'tmp-snap-%s' % self._dst['id'],
                          'size': self._src['size']}

        def __enter__(self):
            self._driver.create_snapshot(self._snap)
            return self

        def get_snap(self):
            return self._snap

        def __exit__(self, type, value, traceback):
            self._driver.delete_snapshot(self._snap)

    class TempRawDevice(object):
        def __init__(self, drv, volume):
            LOG.debug('Creating temporary device: %s'
                      % drv._device_name(volume))
            self._driver = drv
            self._vol = volume

        def __enter__(self):
            self._driver._attach_file(self._vol)
            return self

        def __exit__(self, exc_type, exc_value, exc_tb):
            self._driver._detach_file(self._vol)
            if exc_type is not None:
                six.reraise(exc_type, exc_value, exc_tb)

    class TempLVMDevice(TempRawDevice):
        def __init__(self, *args, **kwargs):
            super(SRBDriver.TempLVMDevice, self).__init__(*args, **kwargs)
            self._vg = None

        def get_vg(self):
            return self._vg

        def __enter__(self):
            super(SRBDriver.TempLVMDevice, self).__enter__()
            self._vg = self._driver._get_lvm_vg(self._vol)
            self._vg.activate_vg()
            return self

        def __exit__(self, exc_type, exc_value, exc_tb):
            super(SRBDriver.TempLVMDevice, self).__exit__(exc_type,
                                                          exc_value, exc_tb)

    def __init__(self, *args, **kwargs):
        super(SRBDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(srb_opts)
        self.urls_setup = False
        self.backend_name = None
        self.base_urls = None
        self._attached_devices = {}

        # NOTE(joachim): Work-around some LVM behavior by monkey-patching
        self._original_lv_activate = lvm.LVM.activate_lv
        lvm.LVM.activate_lv = self._wrap_lv_activate()

    # NOTE(joachim)
    # This is a monkey-patching decorator used to wrap the activate_lv call
    # in order for the LVM class's init to succeed when the lv was already
    # activated (instead of failing miserably when the error could be
    # ignored)
    # summarized: Make brick's lvm activate_lv consider EEXIST as Success
    def _wrap_lv_activate(self):
        def activate(vg, *args, **kwargs):
            try:
                self._original_lv_activate(vg, *args, **kwargs)
            except putils.ProcessExecutionError as err:
                if err.exit_code != 5:
                    raise

        return activate

    def _setup_urls(self):
        if self.base_urls is None or not len(self.base_urls):
            raise exception.VolumeBackendAPIException(
                _LE("No url configured"))

        try:
            cmd = 'echo ' + self.base_urls + ' > /sys/class/srb/add_urls'
            putils.execute('sh', '-c', cmd,
                           root_helper='sudo', run_as_root=True)
            self.urls_setup = True
        except putils.ProcessExecutionError as err:
            LOG.debug('Error creating Volume')
            LOG.debug('Cmd       :%s' % err.cmd)
            LOG.debug('Exit Code :%s' % err.exit_code)
            LOG.debug('StdOut    :%s' % err.stdout)
            LOG.debug('StdErr    :%s' % err.stderr)
            msg = _LE('Could not setup urls on the Block Driver')
            LOG.error(msg)

    def do_setup(self, context):
        """Any initialization the volume driver does while starting."""
        self.backend_name = self.configuration.safe_get('volume_backend_name')
        self.base_urls = self.configuration.safe_get('srb_base_urls')
        self._setup_urls()

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met."""
        if self.base_urls is None or not len(self.base_urls):
            LOG.warning(_LW("Configuration variable srb_base_urls"
                            " not set or empty."))
        if self.urls_setup is False:
            msg = _LE("Could not setup urls properly")
            raise exception.VolumeBackendAPIException(data=msg)

    @staticmethod
    def _is_snapshot(volume):
        return volume['name'].startswith('snapshot')

    @staticmethod
    def _get_volname(volume):
        """Returns the name of the actual volume

        If the volume is a snapshot, it returns the name of the parent volume.
        otherwise, returns the volume's name.
        """
        name = volume['name']
        if SRBDriver._is_snapshot(volume):
            name = "volume-%s" % (volume['volume_id'])
        return name

    @staticmethod
    def _get_volid(volume):
        """Returns the ID of the actual volume

        If the volume is a snapshot, it returns the ID of the parent volume.
        otherwise, returns the volume's id.
        """
        volid = volume['id']
        if SRBDriver._is_snapshot(volume):
            volid = volume['volume_id']
        return volid

    @staticmethod
    def _device_name(volume):
        volume_id = SRBDriver._get_volid(volume)
        # NOTE(Joachim) The device names cannot be longer than 32bytes,
        # se we need to truncate the volume id to fit in, knowing that the
        truncated_id = volume_id[:24]
        return "cinder-%s" % (truncated_id)

    @staticmethod
    def _device_path(volume):
        return "/dev/" + SRBDriver._device_name(volume)

    @staticmethod
    def _escape_snapshot(snapshot_name):
        # Linux LVM reserves name that starts with snapshot, so that
        # such volume name can't be created. Mangle it.
        if not snapshot_name.startswith('snapshot'):
            return snapshot_name
        return '_' + snapshot_name

    @staticmethod
    def _mapper_path(volume):
        groupname = SRBDriver._get_volname(volume)
        name = volume['name']
        if SRBDriver._is_snapshot(volume):
            name = SRBDriver._escape_snapshot(name)
        # NOTE(vish): stops deprecation warning
        groupname = groupname.replace('-', '--')
        name = name.replace('-', '--')
        return "/dev/mapper/%s-%s" % (groupname, name)

    @staticmethod
    def _size_int(size_in_g):
        try:
            if not int(size_in_g):
                return 1
        except ValueError:
            msg = _LE("Invalid size parameter '%s': Cannot be interpreted"
                      " as an integer value") % (size_in_g)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(msg)
        return int(size_in_g)

    @staticmethod
    def _set_device_path(volume):
        volume['provider_location'] = SRBDriver._get_volname(volume)
        return {
            'provider_location': volume['provider_location'],
        }

    def _get_lvm_vg(self, volume, create_vg=False):
        # NOTE(joachim): One-device volume group to manage thin snapshots
        # Get origin volume name even for snapshots
        volume_name = self._get_volname(volume)
        physical_volumes = [self._device_path(volume)]
        return lvm.LVM(volume_name, utils.get_root_helper(),
                       create_vg=create_vg, physical_volumes=physical_volumes,
                       lvm_type='thin', executor=self._execute)

    @staticmethod
    def _volume_not_present(vg, volume_name):
        # Used to avoid failing to delete a volume for which
        # the create operation partly failed
        return vg.get_volume(volume_name) is None

    def _create_file(self, volume):
        try:
            cmd = 'echo ' + volume['name'] + ' '
            cmd += str(self._size_int(volume['size'])
                       * self.OVER_ALLOC_RATIO) + 'G'
            cmd += ' > /sys/class/srb/create'
            putils.execute('/bin/sh', '-c', cmd,
                           root_helper='sudo', run_as_root=True)
        except putils.ProcessExecutionError as err:
            LOG.debug('Error creating Volume')
            LOG.debug('Cmd       :%s' % err.cmd)
            LOG.debug('Exit Code :%s' % err.exit_code)
            LOG.debug('StdOut    :%s' % err.stdout)
            LOG.debug('StdErr    :%s' % err.stderr)
            msg = _LE('Could not create volume on any configured REST server')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(msg)

        return self._set_device_path(volume)

    def _extend_file(self, volume, new_size):
        try:
            cmd = 'echo ' + volume['name'] + ' '
            cmd += str(self._size_int(new_size)
                       * self.OVER_ALLOC_RATIO) + 'G'
            cmd += ' > /sys/class/srb/extend'
            putils.execute('/bin/sh', '-c', cmd,
                           root_helper='sudo', run_as_root=True)
        except putils.ProcessExecutionError as err:
            LOG.debug('Error extending Volume')
            LOG.debug('Cmd       :%s' % err.cmd)
            LOG.debug('Exit Code :%s' % err.exit_code)
            LOG.debug('StdOut    :%s' % err.stdout)
            LOG.debug('StdErr    :%s' % err.stderr)
            msg = _LE('Could not extend volume on any configured REST server')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(msg)

    @staticmethod
    def _destroy_file(volume):
        try:
            cmd = 'echo ' + volume['name'] + ' > /sys/class/srb/destroy'
            putils.execute('/bin/sh', '-c', cmd,
                           root_helper='sudo', run_as_root=True)
        except putils.ProcessExecutionError as err:
            LOG.debug('Error destroying Volume')
            LOG.debug('Cmd       :%s' % err.cmd)
            LOG.debug('Exit Code :%s' % err.exit_code)
            LOG.debug('StdOut    :%s' % err.stdout)
            LOG.debug('StdErr    :%s' % err.stderr)
            msg = _LE('Could not destroy volume on any configured REST server')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(msg)

    # NOTE(joachim): Must only be called within a function decorated by:
    # @synchronized('devices', 'cinder-srb-')
    def _increment_attached_count(self, volume):
        """Increments the attach count of the device"""
        volid = self._get_volid(volume)
        if volid not in self._attached_devices:
            self._attached_devices[volid] = 1
        else:
            self._attached_devices[volid] = self._attached_devices[volid] + 1

    # NOTE(joachim): Must only be called within a function decorated by:
    # @synchronized('devices', 'cinder-srb-')
    def _decrement_attached_count(self, volume):
        """Decrements the attach count of the device"""
        volid = self._get_volid(volume)
        if volid not in self._attached_devices:
            raise exception.VolumeBackendAPIException(
                _LE("Internal error in srb driver: "
                    "Trying to detach detached volume %s")
                % (self._get_volname(volume))
            )

        self._attached_devices[volid] = self._attached_devices[volid] - 1
        if self._attached_devices[volid] == 0:
            del self._attached_devices[volid]

    # NOTE(joachim): Must only be called within a function decorated by:
    # @synchronized('devices', 'cinder-srb-')
    def _get_attached_count(self, volume):
        volid = self._get_volid(volume)
        if volid in self._attached_devices:
            return self._attached_devices[volid]
        return 0

    @synchronized('devices', 'cinder-srb-')
    def _is_attached(self, volume):
        return self._get_attached_count(volume) > 0

    @synchronized('devices', 'cinder-srb-')
    def _attach_file(self, volume):
        name = self._get_volname(volume)
        devname = self._device_name(volume)
        LOG.debug('Attaching volume %s as %s' % (name, devname))

        count = self._get_attached_count(volume)
        if count == 0:
            try:
                cmd = 'echo ' + name + ' ' + devname
                cmd += ' > /sys/class/srb/attach'
                putils.execute('/bin/sh', '-c', cmd,
                               root_helper='sudo', run_as_root=True)
            except putils.ProcessExecutionError as err:
                LOG.debug('Error attaching Volume')
                LOG.debug('Cmd       :%s' % err.cmd)
                LOG.debug('Exit Code :%s' % err.exit_code)
                LOG.debug('StdOut    :%s' % err.stdout)
                LOG.debug('StdErr    :%s' % err.stderr)
                msg = _LE('Could not attach volume %(vol)s'
                          ' as %(dev)s on system')\
                    % {'vol': name, 'dev': devname}
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(msg)

        self._increment_attached_count(volume)

    @retry(times=3, wait_before=True, base_wait=5, increase='increment',
           exceptions=(putils.ProcessExecutionError))
    def _do_deactivate(self, volume, vg):
        vg.deactivate_vg()

    @retry(times=5, wait_before=True, base_wait=1, increase='double',
           exceptions=(putils.ProcessExecutionError))
    def _do_detach(self, volume, vg):
        devname = self._device_name(volume)
        volname = self._get_volname(volume)
        cmd = 'echo ' + devname + ' > /sys/class/srb/detach'
        try:
            putils.execute('/bin/sh', '-c', cmd,
                           root_helper='sudo', run_as_root=True)
        except putils.ProcessExecutionError:
            with excutils.save_and_reraise_exception(reraise=True):
                try:
                    vg.activate_lv(volname)
                    self._do_deactivate(volume, vg)
                except putils.ProcessExecutionError:
                    LOG.warning(_LW("Could not attempt any recovery to"
                                    " retry the failed detach"))


    @synchronized('devices', 'cinder-srb-')
    def _detach_file(self, volume):
        name = self._get_volname(volume)
        devname = self._device_name(volume)
        vg = self._get_lvm_vg(volume)
        LOG.debug('Detaching device %s' % (devname))

        count = self._get_attached_count(volume)
        if count > 1:
            return

        try:
            try:
                if vg is not None:
                    self._do_deactivate(volume, vg)
            except putils.ProcessExecutionError:
                msg = _LE('Could not deactivate volume groupe %s')\
                    % (self._get_volname(volume))
                LOG.error(msg)
                raise

            try:
                self._do_detach(volume, vg=vg)
            except putils.ProcessExecutionError as err:
                msg = _LE('Could not detach volume '
                          '%(vol)s from device %(dev)s')\
                    % {'vol': name, 'dev': devname}
                LOG.error(msg)
                raise

            self._decrement_attached_count(volume)

        except putils.ProcessExecutionError as err:
            LOG.debug('Error detaching Volume')
            LOG.debug('Cmd       :%s' % err.cmd)
            LOG.debug('Exit Code :%s' % err.exit_code)
            LOG.debug('StdOut    :%s' % err.stdout)
            LOG.debug('StdErr    :%s' % err.stderr)
            raise exception.VolumeBackendAPIException(
                _LE('Could not detach volume %(vol)s from device %(dev)s')
                % {'vol': name, 'dev': devname}
            )
        except exception.VolumeBackendAPIException:
            raise

    def _setup_lvm(self, volume):
        # NOTE(joachim): One-device volume group to manage thin snapshots
        size_str = str(self._size_int(volume['size'])
                       * self.OVER_ALLOC_RATIO) + 'g'
        vg = self._get_lvm_vg(volume, create_vg=True)
        vg.create_volume(volume['name'], size_str, lv_type='thin')

    def _destroy_lvm(self, volume):
        vg = self._get_lvm_vg(volume)
        if vg.lv_has_snapshot(volume['name']):
            LOG.error(_LE('Unable to delete due to existing snapshot '
                          'for volume: %s') % volume['name'])
            raise exception.VolumeIsBusy(volume_name=volume['name'])
        vg.destroy_vg()
        # NOTE(joachim) Force lvm vg flush through a vgs command
        vgs = vg.get_all_volume_groups(root_helper='sudo', vg_name=vg.vg_name)
        if len(vgs) != 0:
            LOG.warning(_LW('Removed volume group %s still appears in vgs')
                        % vg.vg_name)

    def _create_n_copy_volume(self, dstvol, srcvol):
        """Creates a volume from a volume or a snapshot."""
        updates = self._create_file(dstvol)

        # We need devices attached for IO operations.
        with nested(self.TempLVMDevice(self, srcvol),
                    self.TempRawDevice(self, dstvol)) as (srcdev, dstdev):
            self._setup_lvm(dstvol)

            # Some configurations of LVM do not automatically activate
            # ThinLVM snapshot LVs.
            vg = srcdev.get_vg()
            vg.activate_lv(srcvol['name'], True)

            # copy_volume expects sizes in MiB, we store integer GiB
            # be sure to convert before passing in
            volutils.copy_volume(self._mapper_path(srcvol),
                                 self._mapper_path(dstvol),
                                 srcvol['volume_size'] * units.Ki,
                                 self.configuration.volume_dd_blocksize,
                                 execute=self._execute)

        return updates

    def create_volume(self, volume):
        """Creates a volume. Can optionally return a Dictionary of
        changes to the volume object to be persisted.
        """
        updates = self._create_file(volume)
        # We need devices attached for LVM operations.
        with self.TempRawDevice(self, volume):
            self._setup_lvm(volume)
        return updates

    def create_volume_from_snapshot(self, volume, snapshot):
        updates = self._create_n_copy_volume(volume, snapshot)

        return updates

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        LOG.info(_LI('Creating clone of volume: %s') % src_vref['id'])

        updates = None
        with self.TempLVMDevice(self, src_vref):
            with self.TempSnapshot(self, volume, src_vref) as temp:
                updates = self._create_n_copy_volume(volume, temp.get_snap())

        return updates

    def delete_volume(self, volume):
        """Deletes a volume."""
        attached = False
        if self._is_attached(volume):
            attached = True
            with self.TempLVMDevice(self, volume):
                self._destroy_lvm(volume)
            self._detach_file(volume)

        LOG.debug('Deleting volume %s, attached=%s'
                  % (volume['name'], attached))

        self._destroy_file(volume)

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        with self.TempLVMDevice(self, snapshot) as dev:
            vg = dev.get_vg()
            # NOTE(joachim) we only want to support thin lvm_types
            vg.create_lv_snapshot(self._escape_snapshot(snapshot['name']),
                                  snapshot['volume_name'],
                                  lv_type='thin')

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""
        with self.TempLVMDevice(self, snapshot) as dev:
            vg = dev.get_vg()
            if self._volume_not_present(vg,
                                        self._escape_snapshot(snapshot['name'])
                                       ):
                # If the snapshot isn't present, then don't attempt to delete
                LOG.warning(_LW("snapshot: %s not found, "
                                "skipping delete operations")
                            % snapshot['name'])
                return

            vg.delete(self._escape_snapshot(snapshot['name']))

    def get_volume_stats(self, refresh=False):
        """Return the current state of the volume service.

        If 'refresh' is True, run the update first.
        """
        stats = {
            'vendor_name': 'Scality - Open Source',
            'driver_version': self.VERSION,
            'storage_protocol': 'Scality Rest Block Device',
            'total_capacity_gb': 'infinite',
            'free_capacity_gb': 'infinite',
            'reserved_percentage': 0,
            'volume_backend_name': self.backend_name,
        }
        return stats

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        """Fetch the image from image_service and write it to the volume."""
        with self.TempLVMDevice(self, volume):
            image_utils.fetch_to_volume_format(context,
                                               image_service,
                                               image_id,
                                               self._mapper_path(volume),
                                               'qcow2',
                                               self.configuration.
                                               volume_dd_blocksize,
                                               size=volume['size'])

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        """Copy the volume to the specified image."""
        with self.TempLVMDevice(self, volume):
            image_utils.upload_volume(context,
                                      image_service,
                                      image_meta,
                                      self._mapper_path(volume))

    def extend_volume(self, volume, new_size):
        new_size_str = str(self._size_int(new_size)
                           * self.OVER_ALLOC_RATIO) + 'g'
        self._extend_file(volume, new_size)
        with self.TempLVMDevice(self, volume) as dev:
            vg = dev.get_vg()
            vg.pv_resize(self._device_path(volume), new_size_str)
            vg.extend_thinpool()
            vg.extend_volume(volume['name'], new_size_str)


class SRBISCSIDriver(SRBDriver, driver.ISCSIDriver):
    """Scality Rest Block's cinder driver with ISCSI export.

    Creates and manages volume files on a REST-based storage service
    for hypervisors to use as block devices through a native linux
    Loadable Kernel Module, and export them through ISCSI to nova.
    """

    VERSION = '0.1.0'

    def __init__(self, *args, **kwargs):
        self.db = kwargs.get('db')
        self.target_helper = self.get_target_helper(self.db)
        super(SRBISCSIDriver, self).__init__(*args, **kwargs)
        self.backend_name =\
            self.configuration.safe_get('volume_backend_name') or 'SRB_iSCSI'
        self.protocol = 'iSCSI'

    def set_execute(self, execute):
        super(SRBISCSIDriver, self).set_execute(execute)
        if self.target_helper is not None:
            self.target_helper.set_execute(execute)

    def ensure_export(self, context, volume):
        self._attach_file(volume)
        vg = self._get_lvm_vg(volume)
        vg.activate_vg()

        volume_name = volume['name']
        iscsi_name = "%s%s" % (self.configuration.iscsi_target_prefix,
                               volume_name)
        device_path = self._mapper_path(volume)
        # NOTE(jdg): For TgtAdm case iscsi_name is the ONLY param we need
        # should clean this all up at some point in the future
        model_update = self.target_helper.ensure_export(
            context, volume,
            iscsi_name,
            device_path,
            None,
            self.configuration)
        if model_update:
            self.db.volume_update(context, volume['id'], model_update)

    def create_export(self, context, volume):
        """Creates an export for a logical volume."""
        # SRB uses the same name as the volume for the VG
        volume_path = "/dev/%s/%s" % (volume['name'], volume['name'])

        data = self.target_helper.create_export(context,
                                                volume,
                                                volume_path,
                                                self.configuration)
        return {
            'provider_location': data['location'],
            'provider_auth': data['auth'],
        }

    def remove_export(self, context, volume):
        # NOTE(joachim) Taken from iscsi._ExportMixin.remove_export
        # This allows us to avoid "detaching" a device not attached by
        # an export, and avoid screwing up the device attach refcount.
        try:
            # Raises exception.NotFound if export not provisioned
            iscsi_target = self.target_helper._get_iscsi_target(context,
                                                                volume['id'])
            # Raises an Exception if currently not exported
            location = volume['provider_location'].split(' ')
            iqn = location[1]
            self.target_helper.show_target(iscsi_target, iqn=iqn)

            self.target_helper.remove_export(context, volume)
            self._detach_file(volume)
        except exception.NotFound:
            pass
        except Exception:
            pass
