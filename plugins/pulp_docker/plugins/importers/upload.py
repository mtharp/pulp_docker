"""
This module contains steps for handling Image uploads. Docker v2 s2 objects (Manifests and Blobs)
saved via skopeo copy can be uploaded at this time.

As of now skopeo copy creates a directory for your image with the blobs and manifest json file.
Therefore, before uploading, the user has to tar the directory contents manually.
Later, when skopeo is capable of creating tar files directly,
then the user can skip the manual step of tarring the contents.

Below is the existing issue raised against the skopeo team.

https://github.com/projectatomic/skopeo/issues/361

If you feel the need to blame this file, don't use git blame. The code was taken from this commit,
which was later negative committed:

https://github.com/pulp/pulp_docker/commit/cc624a1c9ca4fd805e66e0cef646e0302a35da12
"""
from gettext import gettext as _
import contextlib
import functools
import gzip
import json
import logging
import os
import stat
import tarfile

from mongoengine import NotUniqueError

from pulp.plugins.util.publish_step import PluginStep, GetLocalUnitsStep

from pulp_docker.common import constants, error_codes, tarutils
from pulp_docker.plugins import models
from pulp_docker.plugins.importers import v1_sync
from pulp.server.config import config
from pulp.server.content import storage
from pulp.server.controllers import repository
from pulp.server.exceptions import PulpCodedValidationException

_logger = logging.getLogger(__name__)


class UploadStep(PluginStep):
    """
    This is the parent step for Image uploads.
    """

    def __init__(self, repo=None, file_path=None, config=None, metadata=None, type_id=None):
        """
        Initialize the UploadStep, configuring the correct children and setting the description.

        :param repo:      repository to sync
        :type  repo:      pulp.plugins.model.Repository
        :param file_path: The path to the tar file uploaded from a 'docker save'
        :type  file_path: str
        :param config:    plugin configuration for the repository
        :type  config:    pulp.plugins.config.PluginCallConfiguration
        :param metadata:  extra data about the unit
        :type metadata:   dict
        :param type_id:   type of unit being uploaded
        :type type_id:    str
        """
        super(UploadStep, self).__init__(constants.UPLOAD_STEP, repo=repo,
                                         plugin_type=constants.IMPORTER_TYPE_ID,
                                         config=config, disable_reporting=True)
        self.description = _('Uploading Docker Units')

        self.file_path = file_path

        # populated by ProcessMetadata
        self.metadata = None

        # Units that were part of the uploaded tar file, populated by ProcessMetadata
        self.available_units = []

        # populated by ProcessMetadata
        self.v1_tags = {}
        if type_id == models.Image._content_type_id.default:
            self._handle_image()
        elif type_id == models.Tag._content_type_id.default:
            self._handle_tag(metadata)
        elif type_id == models.Manifest._content_type_id.default:
            self._handle_image_manifest()
        else:
            raise NotImplementedError()

    def _handle_image(self):
        """
        Handles the upload of a v1 docker image
        """

        self.add_child(ProcessMetadata(constants.UPLOAD_STEP_METADATA))
        # save this step so its "units_to_download" attribute can be accessed later
        self.v1_step_get_local_units = GetLocalUnitsStep(constants.IMPORTER_TYPE_ID)
        self.add_child(self.v1_step_get_local_units)
        self.add_child(AddImages(step_type=constants.UPLOAD_STEP_SAVE))

    def _handle_tag(self, metadata):
        """
        Handles the upload of a docker tag (tag name and digest)
        """

        self.metadata = metadata
        self.add_child(AddTags(step_type=constants.UPLOAD_STEP_SAVE))

    def _handle_image_manifest(self):
        """
        Handles the upload of a v2 s2 docker image
        """
        self.add_child(ProcessManifest(constants.UPLOAD_STEP_IMAGE_MANIFEST))
        self.v2_step_get_local_units = GetLocalUnitsStep(constants.IMPORTER_TYPE_ID)
        self.add_child(self.v2_step_get_local_units)
        self.add_child(AddUnits(step_type=constants.UPLOAD_STEP_SAVE))


class ProcessMetadata(PluginStep):
    """
    Retrieve metadata from an uploaded tarball and pull out the metadata for further processing.
    """

    def process_main(self, item=None):
        """
        Pull the metadata out of the tar file
        :param item: Not used by this Step
        :type  item: None
        """

        # retrieve metadata from the tarball
        metadata = tarutils.get_metadata(self.parent.file_path)
        # turn that metadata into a collection of models
        mask_id = self.get_config().get(constants.CONFIG_KEY_MASK_ID)
        self.parent.metadata = metadata
        self.parent.available_units = self.get_models(metadata, mask_id)
        self.parent.v1_tags = tarutils.get_tags(self.parent.file_path)

    def get_models(self, metadata, mask_id=''):
        """
        Given image metadata, returns model instances to represent each layer of the image defined
        by the unit_key.

        :param metadata: a dictionary where keys are image IDs, and values are
                         dictionaries with keys "parent" and "size", containing
                         values for those two attributes as taken from the docker
                         image metadata.
        :type  metadata: dict
        :param mask_id:  The ID of an image that should not be included in the
                         returned models. This image and all of its ancestors
                         will be excluded.
        :type  mask_id:  basestring
        :return:         list of models.DockerImage instances
        :rtype:          list
        """
        images = []
        existing_image_ids = set()

        leaf_image_ids = tarutils.get_youngest_children(metadata)

        for image_id in leaf_image_ids:
            while image_id:
                json_data = metadata[image_id]
                parent_id = json_data.get('parent')
                size = json_data['size']

                if image_id not in existing_image_ids:
                    # This will avoid adding multiple images with a same id, which can happen
                    # in case of parents with multiple children.
                    existing_image_ids.add(image_id)
                    images.append(models.Image(image_id=image_id, parent_id=parent_id, size=size))
                if parent_id == mask_id:
                    break
                image_id = parent_id

        return images


class ProcessManifest(PluginStep):
    """
    Retrieves image manifest from an uploaded tarball and pull out the
    metadata for further processing
    """

    def process_main(self):
        """
        Pull the image manifest out of the tar file
        """
        image_manifest = json.dumps(tarutils.get_image_manifest(self.parent.file_path))
        digest = models.UnitMixin.calculate_digest(image_manifest)
        with open(os.path.join(self.get_working_dir(), digest), 'w') as manifest_file:
            manifest_file.write(image_manifest)
        manifest = models.Manifest.from_json(image_manifest, digest)
        self.parent.available_units.append(manifest)
        self.parent.available_units.extend(self.get_models(manifest))

    def get_models(self, manifest):
        """
        Given an image manifest, returns model instances to represent each blob of the image defined
        by the unit_key.

        :param manifest: An initialized Manifest object
        :type manifest:  pulp_docker.plugins.models.Manifest
        :return:         list of models.Blob instances
        :rtype:          list
        """
        available_blobs = set()
        for layer in manifest.fs_layers:
            # skip foreign blobs
            if layer.layer_type == constants.FOREIGN_LAYER:
                continue
            else:
                available_blobs.add(layer.blob_sum)
        if manifest.config_layer:
            available_blobs.add(manifest.config_layer)
        available_blobs = [models.Blob(digest=d) for d in available_blobs]
        return available_blobs


class AddImages(v1_sync.SaveImages):
    """
    Add Images from metadata extracted in the ProcessMetadata step.
    """

    def initialize(self):
        """
        Extract the tarfile to get all the layers from it.
        """
        # Brute force, extract the tar file for now
        with contextlib.closing(tarfile.open(self.parent.file_path)) as archive:
            archive.extractall(self.get_working_dir())

        # fix the permissions so files can be read
        for root, dirs, files in os.walk(self.get_working_dir()):
            for dir_path in dirs:
                os.chmod(os.path.join(root, dir_path),
                         stat.S_IXUSR | stat.S_IWUSR | stat.S_IREAD)
            for file_path in files:
                os.chmod(os.path.join(root, file_path),
                         stat.S_IXUSR | stat.S_IWUSR | stat.S_IREAD)

    def process_main(self, item=None):
        """
        For each layer that we need to save, create the ancestry file then call the parent class to
        finish processing.

        :param item: A docker image unit
        :type  item: pulp_docker.plugins.models.Image
        """
        # Write out the ancestry file
        ancestry = tarutils.get_ancestry(item.image_id, self.parent.metadata)
        layer_dir = os.path.join(self.get_working_dir(), item.image_id)
        with open(os.path.join(layer_dir, 'ancestry'), 'w') as ancestry_fp:
            json.dump(ancestry, ancestry_fp)

        # compress layer.tar to a new file called layer
        layer_src_path = os.path.join(self.get_working_dir(), item.image_id, 'layer.tar')
        layer_dest_path = os.path.join(self.get_working_dir(), item.image_id, 'layer')
        with open(layer_src_path) as layer_src:
            with contextlib.closing(gzip.open(layer_dest_path, 'w')) as layer_dest:
                # these can be big files, so we chunk them
                reader = functools.partial(layer_src.read, 4096)
                for chunk in iter(reader, ''):
                    layer_dest.write(chunk)
        # we don't need layer.tar anymore
        os.remove(layer_src_path)

        super(AddImages, self).process_main(item=item)


class AddTags(PluginStep):
    """
    Create/update tags based on the metadata.
    """

    def process_main(self, item=None):
        """
        Update tags based on the parent metadata

        :param item: Not used by this step
        :type  item: None
        """

        tag = self.parent.metadata['name']
        digest = self.parent.metadata['digest']
        repo_id = self.parent.repo.id
        manifest_type_id = models.Manifest._content_type_id.default
        repo_manifest_ids = repository.get_associated_unit_ids(repo_id, manifest_type_id)

        # check if there is manifest with such id within the queried repo
        # since we don't know if the provided digest is of an image manifest or manifest list
        # we need to try both.
        manifests = models.Manifest.objects.filter(digest=digest, id__in=repo_manifest_ids)
        manifest_type = constants.MANIFEST_IMAGE_TYPE
        if manifests.count() == 0:
            manifest_list_type_id = models.ManifestList._content_type_id.default
            repo_manifest_list_ids = repository.get_associated_unit_ids(
                repo_id, manifest_list_type_id)
            manifests = models.ManifestList.objects.filter(digest=digest,
                                                           id__in=repo_manifest_list_ids)
            manifest_type = constants.MANIFEST_LIST_TYPE
            if manifests.count() == 0:
                raise PulpCodedValidationException(error_code=error_codes.DKR1010,
                                                   digest=digest,
                                                   repo_id=repo_id)

        new_tag = models.Tag.objects.tag_manifest(repo_id=self.parent.repo.id, tag_name=tag,
                                                  manifest_digest=digest,
                                                  schema_version=manifests[0].schema_version,
                                                  manifest_type=manifest_type)

        if new_tag:
            repository.associate_single_unit(self.parent.repo.repo_obj, new_tag)


class AddUnits(PluginStep):
    """
    Add Manifest and Blobs extracted in the ProcessManifest Step
    """
    def initialize(self):
        """
        Extract the tarfile to get all the layers from it.
        """
        # Brute force, extract the tar file for now
        with contextlib.closing(tarfile.open(self.parent.file_path)) as archive:
            archive.extractall(self.get_working_dir())
        self._parse_reference()

    def get_iterator(self):
        """
        Return an iterator that will traverse the list of Units
        that were present in the uploaded tarball.

        :return: An iterable containing the Blobs and Manifests present in the uploaded tarball.
        :rtype:  iterator
        """
        return iter(self.parent.v2_step_get_local_units.units_to_download)

    def process_main(self, item=None):
        """
        Save blobs and manifest to repository

        :param item: A Docker manifest or blob unit
        :type      : pulp_docker.plugins.models.Blob or pulp_docker.plugins.models.Manifest
        :return:     None
        """
        import_path = os.path.join(self.get_working_dir(), item.digest)
        staged_path = None
        if isinstance(item, models.Blob):
            if self._staged_blobs:
                staged_path = os.path.join(self._staged_blobs, "blobs", item.digest)
                if not os.path.exists(staged_path):
                    raise RuntimeError("staged blob %s is missing" % (staged_path,))
            else:
                blob_src_path = os.path.join(self.get_working_dir(),
                                             item.digest.split(':')[1] + '.tar')
                os.rename(blob_src_path, import_path)

        item.set_storage_path(item.digest)
        try:
            item.save()
        except NotUniqueError:
            # blob already imported into pulp
            item = item.__class__.objects.get(**item.unit_key)
        else:
            if staged_path:
                # blob stored nearby and can be renamed into place
                _logger.debug("moving staged blob from %s to %s", staged_path, item.storage_path)
                storage.mkdir(os.path.dirname(item.storage_path))
                os.rename(staged_path, item.storage_path)
            else:
                # blob unpacked from tarball
                item.safe_import_content(import_path)
        repository.associate_single_unit(self.get_repo().repo_obj, item)

    def _parse_reference(self):
        # look for blobs that have been pre-pushed to a location on the filesystem
        try:
            with open(os.path.join(self.get_working_dir(), "reference.json")) as f:
                reference = json.load(f)
        except IOError:
            self._staged_blobs = None
        else:
            ref_path = reference["ref"].split("/", 1)[1]
            ref_path = ref_path.split(":")[0]
            self._staged_blobs = os.path.join(config.get("server", "storage_dir"),
                                              "docker-uploads", ref_path)
            _logger.debug("looking for staged docker blobs at %s", self._staged_blobs)
