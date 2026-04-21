"""Upload action for tiff-to-image-uploader."""

from __future__ import annotations

from plugin.steps import ExtractTiffImagesStep
from synapse_sdk.plugins.actions.upload import (
    DefaultUploadAction,
    UploadContext,
    UploadParams,
)
from synapse_sdk.plugins.steps import StepRegistry


class UploadAction(DefaultUploadAction[UploadParams]):
    """Upload action that extracts images from TIFF files before upload.

    Extends the standard 8-step workflow by inserting an ExtractTiffImagesStep
    after organize_files. The custom step converts multi-frame TIFF files into
    individual image files (PNG/JPG) and replaces organized_files with image entries.

    Extra params (via config.yaml ui_schema):
        - output_format: Output image format (png / jpg)
        - group_name: Group name to assign to all data units
    """

    action_name = 'upload'
    params_model = UploadParams

    def setup_steps(self, registry: StepRegistry[UploadContext]) -> None:
        super().setup_steps(registry)
        registry.insert_after('organize_files', ExtractTiffImagesStep())
