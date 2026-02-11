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
from daily_files.fetching.enumerator import FileRef
from daily_files.ingestion.ingest import IngestedData


class TestSourceRegistry(unittest.TestCase):
    def test_registry_entries_are_source_pipelines(self):
        for key, pipeline in SOURCE_REGISTRY.items():
            self.assertIsInstance(
                pipeline, SourcePipeline, f"{key} is not a SourcePipeline"
            )

    def test_registry_has_expected_sources(self):
        self.assertIn("GSFC", SOURCE_REGISTRY)
        self.assertIn("S6", SOURCE_REGISTRY)
        self.assertIn("S6B", SOURCE_REGISTRY)


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
        self.assertEqual(job.satellite, "GSFC")


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
        job.satellite = job.source_config.satellite
        return job

    def test_acquire_returns_none_when_no_granules(self):
        job = self._make_job()
        job.enumerator_cls = MagicMock(
            return_value=MagicMock(enumerate=MagicMock(return_value=[]))
        )
        job.downloader_cls = MagicMock()
        job.downloader_kwargs = {}
        job.ingestor_cls = MagicMock()

        result = job.acquire("test-bucket")
        self.assertIsNone(result)
        job.downloader_cls.assert_not_called()

    def test_acquire_returns_acquired_data(self):
        job = self._make_job()

        file_refs = [
            FileRef(
                id="1",
                title="f1.nc",
                access_url="s3://b/f1.nc",
                time_start="",
                time_end="",
                modified_time="",
                collection_id="C123",
            ),
        ]
        job.enumerator_cls = MagicMock(
            return_value=MagicMock(enumerate=MagicMock(return_value=file_refs))
        )

        mock_downloader = MagicMock()
        mock_downloader.download_all.return_value = [MagicMock()]
        job.downloader_cls = MagicMock(return_value=mock_downloader)
        job.downloader_kwargs = {}

        mock_ingestor = MagicMock()
        mock_ingestor.ingest.return_value = MagicMock(spec=IngestedData)
        job.ingestor_cls = MagicMock(return_value=mock_ingestor)

        result = job.acquire("test-bucket")
        self.assertIsInstance(result, AcquiredData)
        self.assertEqual(result.granule_titles, ["f1.nc"])
        self.assertEqual(result.collection_ids, ["C123"])
