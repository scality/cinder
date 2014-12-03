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
Volume driver for the Scality REST Block storage system

This driver provisions Linux SRB volumes leveraging RESTful storage platforms
(e.g. Scality CDMI).
"""

import contextlib
import functools
import sys
import time

from oslo.concurrency.lockutils import synchronized
from oslo.concurrency import processutils as putils
from oslo.config import cfg
from oslo.utils import excutils
from oslo.utils import units
import six

from cinder.brick.local_dev import lvm
from cinder import exception
from cinder.i18n import _LI, _LE, _LW
from cinder.image import image_utils
from cinder.openstack.common import log as logging
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


class retry:
    SLEEP_NONE = 'none'
    SLEEP_DOUBLE = 'double'
    SLEEP_INCREMENT = 'increment'

    def __init__(self, exceptions, count, sleep_mechanism=SLEEP_INCREMENT,
                 sleep_factor=1):
        if sleep_mechanism not in [self.SLEEP_NONE,
                                   self.SLEEP_DOUBLE,
                                   self.SLEEP_INCREMENT]:
            raise ValueError('Invalid value for `sleep_mechanism` argument')

        self._exceptions = exceptions
        self._count = count
        self._sleep_mechanism = sleep_mechanism
        self._sleep_factor = sleep_factor

    def __call__(self, fun):
        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            sleep_time = self._sleep_factor
            exc_info = None

            for attempt in xrange(self._count):
                if attempt != 0:
                    LOG.warning(_LW("Retrying failed call, attempt %i")
                                % attempt)

                try:
                    return fun(*args, **kwargs)
                except self._exceptions:
                    exc_info = sys.exc_info()

                if attempt != self._count - 1:
                    if self._sleep_mechanism == self.SLEEP_NONE:
                        continue
                    elif self._sleep_mechanism == self.SLEEP_INCREMENT:
                        time.sleep(sleep_time)
                        sleep_time += self._sleep_factor
                    elif self._sleep_mechanism == self.SLEEP_DOUBLE:
                        time.sleep(sleep_time)
                        sleep_time *= 2
                    else:
                        raise ValueError('Unknown sleep mechanism: %r'
                                         % self._sleep_mechanism)

            six.reraise(exc_info[0], exc_info[1], exc_info[2])

        return wrapped


@contextlib.contextmanager
def patched(obj, attr, fun):
    '''Context manager to locally patch a method.

    Within the managed context, the `attr` method of `obj` will be replaced by
    a method which calls `fun` passing in the original `attr` attribute of
    `obj` as well as any positional and keyword arguments.

    At the end of the context, the original method is restored.
    '''

    orig = getattr(obj, attr)

    def patch(*args, **kwargs):
        return fun(orig, *args, **kwargs)

    setattr(obj, attr, patch)

    try:
        yield
    finally:
        setattr(obj, attr, orig)


@contextlib.contextmanager
def handle_process_execution_error(message, info_message, reraise=True):
    '''Consistently handle `putils.ProcessExecutionError` exceptions

    This context-manager will catch any `putils.ProcessExecutionError`
    exceptions raised in the managed block, and generate logging output
    accordingly.

    The value of the `message` argument will be logged at `logging.ERROR`
    level, and the `info_message` argument at `logging.INFO` level. Finally
    the command string, exit code, standard output and error output of the
    process will be logged at `logging.DEBUG` level.

    The `reraise` argument specifies what should happend when a
    `putils.ProcessExecutionError` is caught. If it's equal to `True`, the
    exception will be re-raised. If it's some other non-`False` object, this
    object will be raised instead (so you most likely want it to be some
    `Exception`). Any `False` value will result in the exception to be
    swallowed.
    '''

    try:
        yield
    except putils.ProcessExecutionError as exc:
        LOG.error(message)

        LOG.info(info_message)
        LOG.debug('Command   : %s', exc.cmd)
        LOG.debug('Exit Code : %r', exc.exit_code)
        LOG.debug('StdOut    : %s', exc.stdout)
        LOG.debug('StdErr    : %s', exc.stderr)

        if reraise is True:
            raise
        elif reraise:
            raise reraise


@contextlib.contextmanager
def temp_snapshot(driver, volume, src_vref):
    snapshot = {'volume_name': src_vref['name'],
                'volume_id': src_vref['id'],
                'volume_size': src_vref['size'],
                'name': 'snapshot-clone-%s' % volume['id'],
                'id': 'tmp-snap-%s' % volume['id'],
                'size': src_vref['size']}

    driver.create_snapshot(snapshot)

    try:
        yield snapshot
    finally:
        driver.delete_snapshot(snapshot)


@contextlib.contextmanager
def temp_raw_device(driver, volume):
    driver._attach_file(volume)

    try:
        yield
    finally:
        driver._detach_file(volume)


@contextlib.contextmanager
def temp_lvm_device(driver, volume):
    with temp_raw_device(driver, volume):
        vg = driver._get_lvm_vg(volume)
        vg.activate_vg()

        yield vg


class SRBDriver(driver.VolumeDriver):
    """Scality SRB volume driver

    This driver manages volumes provisioned by the Scality REST Block driver
    Linux kernel module, backed by RESTful storage providers (e.g. Scality
    CDMI).
    """

    VERSION = '0.1.0'

    OVER_ALLOC_RATIO = 2

    def __init__(self, *args, **kwargs):
        super(SRBDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(srb_opts)
        self.urls_setup = False
        self.backend_name = None
        self.base_urls = None
        self._attached_devices = {}

    def _setup_urls(self):
        if self.base_urls is None or not len(self.base_urls):
            message = "No url configured"
            raise exception.VolumeBackendAPIException(data=message)

        with handle_process_execution_error(
                message=_LE('Cound not setup urls on the Block Driver'),
                info_message=_LI('Error creating Volume'),
                reraise=False):
            cmd = 'echo ' + self.base_urls + ' > /sys/class/srb/add_urls'
            putils.execute('sh', '-c', cmd,
                           root_helper='sudo', run_as_root=True)
            self.urls_setup = True

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
            message = "Could not setup urls properly"
            raise exception.VolumeBackendAPIException(data=message)

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
        name = 'cinder-%s' % volume_id

        # Device names can't be longer than 32 bytes
        return name[:32]

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
            message = ("Invalid size parameter '%s': Cannot be interpreted"
                       " as an integer value"
                       % (size_in_g))
            LOG.error(message)
            raise exception.VolumeBackendAPIException(data=message)
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

        # Patch `lvm.LVM.activate_lv` in order to let the `lvm.LVM` constructor
        # succeed, even when the LV is already activated.

        def activate_lv(orig, *args, **kwargs):
            try:
                orig(*args, **kwargs)
            except putils.ProcessExecutionError as exc:
                if exc.exit_code != 5:
                    raise

        with patched(lvm.LVM, 'activate_lv', activate_lv):
            return lvm.LVM(volume_name, utils.get_root_helper(),
                           create_vg=create_vg,
                           physical_volumes=physical_volumes,
                           lvm_type='thin', executor=self._execute)

    @staticmethod
    def _volume_not_present(vg, volume_name):
        # Used to avoid failing to delete a volume for which
        # the create operation partly failed
        return vg.get_volume(volume_name) is None

    def _create_file(self, volume):
        message = 'Could not create volume on any configured REST server'

        with handle_process_execution_error(
                message=message,
                info_message=_LI('Error creating Volume'),
                reraise=exception.VolumeBackendAPIException(data=message)):
            cmd = 'echo ' + volume['name'] + ' '
            cmd += str(self._size_int(volume['size'])
                       * self.OVER_ALLOC_RATIO) + 'G'
            cmd += ' > /sys/class/srb/create'
            putils.execute('/bin/sh', '-c', cmd,
                           root_helper='sudo', run_as_root=True)

        return self._set_device_path(volume)

    def _extend_file(self, volume, new_size):
        message = 'Could not extend volume on any configured REST server'

        with handle_process_execution_error(
                message=message,
                info_message=_LI('Error extending Volume'),
                reraise=exception.VolumeBackendAPIException(data=message)):
            cmd = 'echo ' + volume['name'] + ' '
            cmd += str(self._size_int(new_size)
                       * self.OVER_ALLOC_RATIO) + 'G'
            cmd += ' > /sys/class/srb/extend'
            putils.execute('/bin/sh', '-c', cmd,
                           root_helper='sudo', run_as_root=True)

    @staticmethod
    def _destroy_file(volume):
        message = 'Could not destroy volume on any configured REST server'

        with handle_process_execution_error(
                message=message,
                info_message=_LI('Error destroying Volume'),
                reraise=exception.VolumeBackendAPIException(data=message)):
            cmd = 'echo ' + volume['name'] + ' > /sys/class/srb/destroy'
            putils.execute('/bin/sh', '-c', cmd,
                           root_helper='sudo', run_as_root=True)

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
            message = ('Could not attach volume %(vol)s as %(dev)s on system'
                       % {'vol': name, 'dev': devname})
            with handle_process_execution_error(
                    message=message,
                    info_message=_LI('Error attaching Volume'),
                    reraise=exception.VolumeBackendAPIException(data=message)):
                cmd = 'echo ' + name + ' ' + devname
                cmd += ' > /sys/class/srb/attach'
                putils.execute('/bin/sh', '-c', cmd,
                               root_helper='sudo', run_as_root=True)

        self._increment_attached_count(volume)

    @retry(exceptions=(putils.ProcessExecutionError, ),
           count=3, sleep_mechanism=retry.SLEEP_INCREMENT, sleep_factor=5)
    def _do_deactivate(self, volume, vg):
        vg.deactivate_vg()

    @retry(exceptions=(putils.ProcessExecutionError, ),
           count=5, sleep_mechanism=retry.SLEEP_DOUBLE, sleep_factor=1)
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

        message = ('Could not detach volume %(vol)s from device %(dev)s'
                   % {'vol': name, 'dev': devname})
        with handle_process_execution_error(
                message=message,
                info_message=_LI('Error detaching Volume'),
                reraise=exception.VolumeBackendAPIException(data=message)):
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
            except putils.ProcessExecutionError:
                msg = _LE('Could not detach volume '
                          '%(vol)s from device %(dev)s')\
                    % {'vol': name, 'dev': devname}
                LOG.error(msg)
                raise

            self._decrement_attached_count(volume)

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
        with temp_lvm_device(self, srcvol) as vg, \
                temp_raw_device(self, dstvol):
            self._setup_lvm(dstvol)

            # Some configurations of LVM do not automatically activate
            # ThinLVM snapshot LVs.
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
        with temp_raw_device(self, volume):
            self._setup_lvm(volume)
        return updates

    def create_volume_from_snapshot(self, volume, snapshot):
        updates = self._create_n_copy_volume(volume, snapshot)

        return updates

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        LOG.info(_LI('Creating clone of volume: %s') % src_vref['id'])

        updates = None
        with temp_lvm_device(self, src_vref):
            with temp_snapshot(self, volume, src_vref) as snapshot:
                updates = self._create_n_copy_volume(volume, snapshot)

        return updates

    def delete_volume(self, volume):
        """Deletes a volume."""
        attached = False
        if self._is_attached(volume):
            attached = True
            with temp_lvm_device(self, volume):
                self._destroy_lvm(volume)
            self._detach_file(volume)

        LOG.debug('Deleting volume %s, attached=%s'
                  % (volume['name'], attached))

        self._destroy_file(volume)

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        with temp_lvm_device(self, snapshot) as vg:
            # NOTE(joachim) we only want to support thin lvm_types
            vg.create_lv_snapshot(self._escape_snapshot(snapshot['name']),
                                  snapshot['volume_name'],
                                  lv_type='thin')

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""
        with temp_lvm_device(self, snapshot) as vg:
            if self._volume_not_present(
                    vg, self._escape_snapshot(snapshot['name'])):
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
        with temp_lvm_device(self, volume):
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
        with temp_lvm_device(self, volume):
            image_utils.upload_volume(context,
                                      image_service,
                                      image_meta,
                                      self._mapper_path(volume))

    def extend_volume(self, volume, new_size):
        new_size_str = str(self._size_int(new_size)
                           * self.OVER_ALLOC_RATIO) + 'g'
        self._extend_file(volume, new_size)
        with temp_lvm_device(self, volume) as vg:
            vg.pv_resize(self._device_path(volume), new_size_str)
            vg.extend_thin_pool()
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
