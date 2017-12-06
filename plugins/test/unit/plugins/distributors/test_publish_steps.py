from collections import defaultdict
import datetime
import os
import shutil
import tempfile
import unittest

import mock

from pulp_docker.plugins import models
from pulp_docker.plugins.distributors import publish_steps


class Base(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.working_dir = os.path.join(self.temp_dir, 'working')
        self.publish_dir = os.path.join(self.temp_dir, 'publish')
        self.content_dir = os.path.join(self.temp_dir, 'content')
        os.makedirs(self.working_dir)
        os.makedirs(self.publish_dir)
        os.makedirs(self.content_dir)
        self.repo = mock.MagicMock(id='foo', working_dir=self.working_dir)
        self.conduit = mock.MagicMock()
        self.config = mock.MagicMock()
        self._distr_get_working_dir = mock.patch(
            "pulp_docker.plugins.distributors.publish_steps.publish_step.common_utils.get_working_directory")
        self._distr_get_working_dir.return_value = self.working_dir
        self._distr_get_working_dir.start()
        self.units = []

    def tearDown(self):
        self.units = []
        self._distr_get_working_dir.stop()
        shutil.rmtree(self.temp_dir)

    @classmethod
    def patch_class(cls, class_name):
        return mock.patch('pulp_docker.plugins.distributors.publish_steps.%s' % class_name)


class TestWebPublisher(Base):
    @mock.patch('pulp_docker.plugins.distributors.publish_steps.Distributor')
    @mock.patch('pulp_docker.plugins.distributors.publish_steps.V2MultiWebPublisher')
    @mock.patch('pulp_docker.plugins.distributors.publish_steps.V2WebPublisher')
    @mock.patch('pulp_docker.plugins.distributors.publish_steps.v1_publish_steps.WebPublisher')
    def test_init(self, v1webpub, v2webpub, v2multiwebpub, distrib):
        distrib.objects.get_or_404.return_value = dict(
            last_publish=datetime.datetime(2013, 12, 11, 10, 9, 8))
        predistributor_id = mock.MagicMock(last_publish="yesteryear")
        self.config.flatten.return_value = dict(predistributor_id=predistributor_id)
        publisher = publish_steps.WebPublisher(self.repo, self.conduit, self.config)
        self.assertEquals(
            [v1webpub.return_value, v2multiwebpub.return_value],
            publisher.children)
        distrib.objects.get_or_404.assert_called_once_with(
            distributor_id=predistributor_id, repo_id="foo")


class TestV2MultiWebPublisher(Base):
    @Base.patch_class("publish_step.AtomicDirectoryPublishStep")
    @Base.patch_class("V2MultiRedirectFilesStep")
    @Base.patch_class("V2MultiPublishTagsStep")
    @Base.patch_class("V2MultiPublishBlobsStep")
    @Base.patch_class("V2MultiPublishManifestsStep")
    @Base.patch_class("V2MultiCollectTagsStep")
    def test_children(self, _step_collect_tags, _step_pub_manifest,
                      _step_pub_blobs, _step_pub_tags, _step_pub_redir,
                      _step_atomic_dir):
        # Make sure the proper steps get initialized
        q = mock.MagicMock()
        step = publish_steps.V2MultiWebPublisher(
            self.repo, self.conduit, self.config, repo_content_unit_q=q)
        self.assertEquals(
            [
                _step_collect_tags.return_value,
                _step_pub_manifest.return_value,
                _step_pub_blobs.return_value,
                _step_pub_tags.return_value,
                _step_pub_redir.return_value,
                _step_atomic_dir.return_value,
            ],
            step.children)
        _step_collect_tags.assert_called_once_with()
        _step_pub_manifest.assert_called_once_with(q)
        _step_pub_blobs.assert_called_once_with(repo_content_unit_q=q)
        _step_pub_tags.assert_called_once_with()
        _step_pub_redir.assert_called_once_with()

    def setup_repo_controller(self, controller):
        uclass_to_type_id = dict()
        unit_dict = defaultdict(list)
        for unit in self.units:
            klass = unit.__class__
            unit_dict.setdefault(klass, []).append(unit)
            uclass_to_type_id[klass] = unit.type_id
        unit_counts = defaultdict(lambda: 0)
        unit_counts.update((uclass_to_type_id[klass], len(units))
                           for klass, units in unit_dict.items()
                           if klass in uclass_to_type_id)

        def mock_get_units(repo_id, model_class, *args, **kwargs):
            units = unit_dict[model_class]
            query = mock.MagicMock()
            query.count.return_value = len(units)
            query.__iter__.return_value = iter(units)
            return [query]
        controller.get_unit_model_querysets.side_effect = mock_get_units
        self.repo.content_unit_counts = unit_counts

    Tag_foo_123_latest = models.Tag(
        manifest_digest="sha256:123", name="foo:latest",
        schema_version=2)
    Tag_foo_123_123 = models.Tag(
        manifest_digest="sha256:123", name="foo:1.2.3",
        schema_version=2)
    Tag_foo_456_latest = models.Tag(
        manifest_digest="sha256:456", name="foo:latest",
        schema_version=2)
    Tag_foo_456_456 = models.Tag(
        manifest_digest="sha256:456", name="foo:4.5.6",
        schema_version=2)
    Tag_bar_123_latest = models.Tag(
        manifest_digest="sha256:123", name="bar:latest",
        schema_version=2)
    Tag_baz_789_latest = models.Tag(
        manifest_digest="sha256:789", name="baz:latest",
        schema_version=2)

    @mock.patch('pulp.plugins.util.publish_step.repo_controller')
    def test_step_collect_tags(self, repo_controller):
        parent = mock.MagicMock(manifest_to_imgname_to_tags=dict(),
                                imgname_to_tags=dict())
        step = publish_steps.V2MultiCollectTagsStep()
        step.parent = parent

        self.units = [
            self.Tag_foo_123_latest,
            self.Tag_foo_123_123,
            self.Tag_foo_456_latest,
            self.Tag_foo_456_456,
            self.Tag_bar_123_latest,
            self.Tag_baz_789_latest,
        ]
        self.setup_repo_controller(repo_controller)

        step.process_lifecycle()
        self.assertEquals(
            {
                'foo': set(["1.2.3", "4.5.6", "latest"]),
                'bar': set(["latest"]),
                'baz': set(["latest"]),
            },
            parent.imgname_to_tags)
        self.assertEquals(
            {
                "sha256:123": {
                    "foo": [("latest", self.Tag_foo_123_latest),
                            ("1.2.3", self.Tag_foo_123_123)],
                    "bar": [("latest", self.Tag_bar_123_latest)],
                },
                "sha256:456": {
                    "foo": [("latest", self.Tag_foo_456_latest),
                            ("4.5.6", self.Tag_foo_456_456)],
                },
                "sha256:789": {
                    "baz": [("latest", self.Tag_baz_789_latest)],
                },
            },
            parent.manifest_to_imgname_to_tags)

    def _Manifest(self, digest, layers=None, config_layer=None):
        if layers is None:
            fs_layers = []
        else:
            fs_layers = [models.FSLayer(blob_sum=x) for x in layers]
        fname = os.path.join(self.content_dir, digest)
        open(fname, "w")
        m = models.Manifest(digest=digest, schema_version=2,
                            fs_layers=fs_layers, config_layer=config_layer)
        m._storage_path = fname
        return m

    @mock.patch('pulp.plugins.util.publish_step.repo_controller')
    def test_step_publish_manifests(self, repo_controller):
        manifest_to_imgname_to_tags = {
                "sha256:123": {
                    "foo": [("latest", self.Tag_foo_123_latest),
                            ("1.2.3", self.Tag_foo_123_123)],
                    "bar": [("latest", self.Tag_bar_123_latest)],
                },
                "sha256:456": {
                    "foo": [("latest", self.Tag_foo_456_latest),
                            ("4.5.6", self.Tag_foo_456_456)],
                },
                "sha256:789": {
                    "baz": [("latest", self.Tag_baz_789_latest)],
                },
            }
        parent = mock.MagicMock(
            manifest_to_imgname_to_tags=manifest_to_imgname_to_tags,
            imgname_to_mfid=dict(),
            layer_to_manifests=dict(),
        )
        parent.get_working_dir.return_value = self.working_dir
        step = publish_steps.V2MultiPublishManifestsStep(self.repo.id)
        step.parent = parent

        MF_123 = self._Manifest(digest="sha256:123", layers=["l1", "l2"],
                                config_layer="c1")
        MF_456 = self._Manifest(digest="sha256:456", layers=["l1", "l3"])
        MF_789 = self._Manifest(digest="sha256:789", layers=["l4"],
                                config_layer="c3")
        self.units = [MF_123, MF_456, MF_789]
        self.setup_repo_controller(repo_controller)

        step.process_lifecycle()

        self.assertEquals(
            {
                'foo': set([MF_123.digest, MF_456.digest]),
                'bar': set([MF_123.digest]),
                'baz': set([MF_789.digest]),
            },
            parent.imgname_to_mfid)
        self.assertEquals(
            {
                "l1": set([MF_123.digest, MF_456.digest]),
                "l2": set([MF_123.digest]),
                "l3": set([MF_456.digest]),
                "l4": set([MF_789.digest]),
                "c1": set([MF_123.digest]),
                "c3": set([MF_789.digest]),
            },
            parent.layer_to_manifests)
        # Make sure symlinks got created
        symlinks = [
            ("foo", "1.2.3", "sha256:123"),
            ("foo", "sha256:123", "sha256:123"),
            ("foo", "4.5.6", "sha256:456"),
            ("foo", "latest", "sha256:456"),
            ("foo", "sha256:456", "sha256:456"),
            ("bar", "latest", "sha256:123"),
            ("bar", "sha256:123", "sha256:123"),
            ("baz", "latest", "sha256:789"),
            ("baz", "sha256:789", "sha256:789"),
        ]
        for img_name, img_tag, mf_id in symlinks:
            spath = os.path.join(self.working_dir, img_name, "manifests", "2",
                                 img_tag)
            self.assertTrue(os.path.exists(spath))
            dst = os.path.join(self.content_dir, mf_id)
            self.assertEquals(dst, os.readlink(spath))

    def _Blob(self, digest):
        fname = os.path.join(self.content_dir, digest)
        open(fname, "w")
        m = models.Blob(digest=digest)
        m._storage_path = fname
        return m

    @mock.patch('pulp.plugins.util.publish_step.repo_controller')
    def test_step_publish_blobs(self, repo_controller):
        manifest_to_imgname_to_tags = {
                "sha256:123": {
                    "foo": [("latest", self.Tag_foo_123_latest),
                            ("1.2.3", self.Tag_foo_123_123)],
                    "bar": [("latest", self.Tag_bar_123_latest)],
                },
                "sha256:456": {
                    "foo": [("latest", self.Tag_foo_456_latest),
                            ("4.5.6", self.Tag_foo_456_456)],
                },
                "sha256:789": {
                    "baz": [("latest", self.Tag_baz_789_latest)],
                },
            }
        layer_to_manifests = {
            "l1": set(["sha256:123", "sha256:456"]),
            "l2": set(["sha256:123"]),
            "l3": set(["sha256:456"]),
            "l4": set(["sha256:789"]),
            "c1": set(["sha256:123"]),
            "c2": set(["sha256:123"]),
            "c3": set(["sha256:456"]),
        }
        parent = mock.MagicMock(
            manifest_to_imgname_to_tags=manifest_to_imgname_to_tags,
            layer_to_manifests=layer_to_manifests,
        )
        parent.get_working_dir.return_value = self.working_dir
        step = publish_steps.V2MultiPublishBlobsStep(self.repo.id)
        step.parent = parent

        Blob_1 = self._Blob(digest="l1")
        Blob_2 = self._Blob(digest="l2")
        Blob_3 = self._Blob(digest="l3")
        Blob_4 = self._Blob(digest="l4")
        Blob_c1 = self._Blob(digest="c1")
        Blob_c2 = self._Blob(digest="c2")
        Blob_c3 = self._Blob(digest="c3")
        self.units = [Blob_1, Blob_2, Blob_3, Blob_4, Blob_c1, Blob_c2, Blob_c3]
        self.setup_repo_controller(repo_controller)

        step.process_lifecycle()

        # Make sure symlinks got created
        symlinks = [
            ("foo", ["c1", "c2", "c3", "l1", "l2", "l3"]),
            ("bar", ["c1", "c2", "l1", "l2"]),
            ("baz", ["l4"]),
        ]
        for img_name, blob_ids in symlinks:
            for blob_id in blob_ids:
                spath = os.path.join(self.working_dir, img_name, "blobs", blob_id)
                self.assertTrue(os.path.exists(spath))
                dst = os.path.join(self.content_dir, blob_id)
                self.assertEquals(dst, os.readlink(spath))

    @mock.patch('pulp.plugins.util.publish_step.repo_controller')
    def test_step_publish_tags(self, repo_controller):
        imgname_to_tags = {
            'foo': set(["1.2.3", "4.5.6", "latest"]),
            'bar': set(["latest"]),
            'baz': set(["latest"])}
        parent = mock.MagicMock(imgname_to_tags=imgname_to_tags)
        repo_registry_id = 'onepm/onepm-rest'
        parent.get_config.return_value = {'repo-registry-id': repo_registry_id}

        parent.get_working_dir.return_value = self.working_dir
        parent.get_repo.return_value = self.repo
        step = publish_steps.V2MultiPublishTagsStep()
        step.parent = parent

        step.process_lifecycle()

        # Make sure tag files got created
        for img_name, tags in imgname_to_tags.items():
            spath = os.path.join(self.working_dir, img_name, "tags", "list")
            self.assertEquals(
                {
                    'name': '%s/%s' % (repo_registry_id, img_name),
                    'tags': sorted(tags),
                },
                publish_steps.json.load(open(spath)))

    @mock.patch('pulp_docker.plugins.distributors.publish_steps.configuration.server_config')
    @mock.patch('pulp.plugins.util.publish_step.repo_controller')
    def test_step_redirect_files(self, repo_controller, _server_config):
        # redirect file has the server name in it, we need to mock it
        _server_config.get.side_effect = dict(server='example.com').get

        imgname_to_tags = {
            'foo': set(["1.2.3", "4.5.6", "latest"]),
            'bar': set(["latest"]),
            'baz': set(["latest"])}
        imgname_to_mfid = {
            'foo': set(["sha256:123", "sha256:456"]),
            'bar': set(["sha256:123"]),
            'baz': set(["sha256:789"])}
        parent = mock.MagicMock(imgname_to_tags=imgname_to_tags,
                                imgname_to_mfid=imgname_to_mfid,
                                docker_api_version="v2")
        repo_registry_id = 'onepm/onepm-rest'
        parent.get_config.return_value = {
            'repo-registry-id': repo_registry_id,
            'docker_publish_directory': self.publish_dir,
        }
        parent.atomic_publish_step.publish_locations = publish_locations = []

        parent.get_working_dir.return_value = self.working_dir
        parent.get_repo.return_value = self.repo
        step = publish_steps.V2MultiRedirectFilesStep()
        step.parent = parent

        step.process_lifecycle()

        common = dict(
            version=4, manifest_list_amd64_tags=dict(),
            protected=False, type="pulp-docker-redirect",
            manifest_list_data=[])

        # Make sure tag files got created
        for img_name, tags in imgname_to_tags.items():
            spath = os.path.join(self.working_dir, "app", "%s.json" %
                                 img_name)
            refs = sorted(tags.union(imgname_to_mfid[img_name]))
            self.assertEquals(
                dict(
                    common,
                    schema2_data=refs,
                    repository=self.repo.id,
                    url="https://example.com/pulp/docker/v2/%s/%s" % (
                        self.repo.id, img_name),
                    **{"repo-registry-id": "%s/%s" % (repo_registry_id, img_name)}),
                publish_steps.json.load(open(spath)))
        # Make sure we registered the redirect file location with the atomic
        # dir step
        self.assertEquals(
            [("app", os.path.join(self.publish_dir, "v2", "app", repo_registry_id))],
            publish_locations)
