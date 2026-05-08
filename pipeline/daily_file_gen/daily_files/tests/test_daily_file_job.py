import unittest
from unittest.mock import patch, MagicMock
from daily_files.config.source_config import get_source_config
from datetime import datetime

from daily_files.daily_file_job import (
    DailyFileJob,
    SourceNotSupported,
    SourcePipeline,
    SOURCE_REGISTRY,
    AcquiredData,
    _get_output_filename,
)
from daily_files.fetching.downloader import S3Downloader
from daily_files.ingestion.ingest import IngestedData


class TestSourceRegistry(unittest.TestCase):
    def test_registry_entries_are_source_pipelines(self):
        for key, pipeline in SOURCE_REGISTRY.items():
            self.assertIsInstance(pipeline, SourcePipeline, f"{key} is not a SourcePipeline")

    def test_registry_has_expected_sources(self):
        self.assertIn("GSFC", SOURCE_REGISTRY)
        self.assertIn("S6", SOURCE_REGISTRY)
        self.assertIn("S6B", SOURCE_REGISTRY)
        self.assertIn("EXAMPLE_S3", SOURCE_REGISTRY)

    def test_pipeline_has_no_enumerator_attribute(self):
        pipeline = SOURCE_REGISTRY["S6"]
        self.assertFalse(hasattr(pipeline, "enumerator"))

    def test_example_s3_uses_iam_credentials(self):
        pipeline = SOURCE_REGISTRY["EXAMPLE_S3"]
        self.assertIs(pipeline.downloader, S3Downloader)
        self.assertIsNone(pipeline.downloader_kwargs["credentials_fn"])


class TestDailyFileJobInit(unittest.TestCase):
    @patch("daily_files.daily_file_job.get_source_config")
    def test_unsupported_source_raises(self, mock_get_config):
        mock_get_config.return_value = MagicMock()
        with self.assertRaises(SourceNotSupported):
            DailyFileJob("2023-12-17", "BOGUS")

    def test_valid_source_initializes(self):
        job = DailyFileJob("2023-12-17", "GSFC")
        self.assertEqual(job.date, datetime(2023, 12, 17))
        self.assertEqual(job.source, "GSFC")


class TestGetOutputFilename(unittest.TestCase):
    def test_filename_format(self):
        job = DailyFileJob("2023-12-17", "GSFC")
        filename = _get_output_filename(job)
        self.assertIn("GSFC", filename)
        self.assertIn("20231217", filename)
        self.assertTrue(filename.endswith(".nc"))


class TestAcquirePhase(unittest.TestCase):
    def _make_job(self):
        job = DailyFileJob.__new__(DailyFileJob)
        job.date = datetime(2023, 12, 17)
        job.source = "GSFC"
        job.source_config = get_source_config("GSFC")
        return job

    def test_acquire_returns_none_when_no_granules(self):
        job = self._make_job()
        job.downloader_cls = MagicMock()
        job.downloader_kwargs = {}
        job.ingestor_cls = MagicMock()

        result = job.acquire([], "test-bucket")
        self.assertIsNone(result)
        job.downloader_cls.assert_not_called()

    def test_acquire_returns_acquired_data(self):
        job = self._make_job()

        granules = ["s3://bucket/path/to/f1.nc"]

        mock_downloader = MagicMock()
        mock_downloader.download_all.return_value = [MagicMock()]
        job.downloader_cls = MagicMock(return_value=mock_downloader)
        job.downloader_kwargs = {}

        mock_ingestor = MagicMock()
        mock_ingestor.ingest.return_value = MagicMock(spec=IngestedData)
        job.ingestor_cls = MagicMock(return_value=mock_ingestor)

        result = job.acquire(granules, "test-bucket")
        self.assertIsInstance(result, AcquiredData)
        self.assertEqual(result.granule_filenames, ["f1.nc"])
        mock_downloader.download_all.assert_called_once_with(granules)
        # Ingestor receives filename list and bucket as kwargs
        _, kwargs = mock_ingestor.ingest.call_args
        self.assertEqual(kwargs["filenames"], ["f1.nc"])
        self.assertEqual(kwargs["bucket"], "test-bucket")
