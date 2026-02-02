"""Custom steps for tiff-to-image upload plugin."""

from __future__ import annotations

import io
import os
import shutil
from pathlib import Path
from typing import Any

from PIL import Image

from synapse_sdk.plugins.actions.upload.context import UploadContext
from synapse_sdk.plugins.steps import BaseStep, StepResult


class ExtractTiffImagesStep(BaseStep[UploadContext]):
    """Extract images from TIFF files and replace organized_files with image entries.

    Multi-frame TIFF files are split into individual frames. Each frame is saved
    as a separate image file (PNG or JPG) with TIFF metadata preserved.

    Reads extra_params from context:
        - output_format (str): Output image format ('png' or 'jpg'). Default: 'png'.
        - group_name (str | None): Group name to assign to all data units.
    """

    TIFF_EXTENSIONS = {'.tif', '.tiff'}

    @property
    def name(self) -> str:
        return 'extract_tiff_images'

    @property
    def progress_weight(self) -> float:
        return 0.15

    def can_skip(self, context: UploadContext) -> bool:
        """Skip if no TIFF files found in organized_files."""
        for file_group in context.organized_files:
            for file_path in file_group.get('files', {}).values():
                if isinstance(file_path, list):
                    file_path = file_path[0] if file_path else None
                if file_path and Path(file_path).suffix.lower() in self.TIFF_EXTENSIONS:
                    return False
        return True

    def execute(self, context: UploadContext) -> StepResult:
        extra = context.params.get('extra_params') or {}
        output_format = extra.get('output_format', 'png')
        group_name = extra.get('group_name')

        temp_dir = self._create_temp_directory(context)
        processed_files: list[dict[str, Any]] = []
        total_images_extracted = 0

        try:
            for file_group in context.organized_files:
                files_dict = file_group.get('files', {})
                meta = file_group.get('meta', {})

                for spec_name, file_path in files_dict.items():
                    if isinstance(file_path, list):
                        file_path = file_path[0] if file_path else None
                    if file_path is None:
                        continue

                    file_path = Path(file_path)
                    if file_path.suffix.lower() not in self.TIFF_EXTENSIONS:
                        processed_files.append(file_group)
                        continue

                    extracted_images, tiff_metadata = self._extract_images(
                        file_path, temp_dir, output_format, context,
                    )

                    if not extracted_images:
                        context.log(
                            'tiff_image_extraction_skip',
                            {'file': file_path.name, 'reason': 'no images extracted'},
                        )
                        continue

                    frame_count = len(extracted_images)
                    for i, image_path in enumerate(extracted_images):
                        frame_meta: dict[str, Any] = {
                            **meta,
                            'origin_file_name': file_path.name,
                            'origin_file_format': file_path.suffix.lstrip('.').lower(),
                            'origin_tiff_path': str(file_path),
                            **tiff_metadata,
                            'frame_count': frame_count,
                            'frame_index': i + 1,
                            'output_format': output_format,
                        }

                        entry: dict[str, Any] = {
                            'files': {spec_name: Path(image_path)},
                            'meta': frame_meta,
                        }
                        if group_name:
                            entry['groups'] = [group_name]

                        processed_files.append(entry)

                    total_images_extracted += frame_count

            context.organized_files = processed_files

            context.params['cleanup_temp'] = True
            context.params['temp_path'] = str(temp_dir)

            context.log(
                'tiff_image_extraction_complete',
                {'total_images': total_images_extracted, 'total_entries': len(processed_files)},
            )

            return StepResult(
                success=True,
                data={'images_extracted': total_images_extracted},
                rollback_data={'temp_dir': str(temp_dir)},
            )

        except Exception as e:
            return StepResult(success=False, error=f'TIFF image extraction failed: {e}')

    def rollback(self, context: UploadContext, result: StepResult) -> None:
        temp_dir = result.rollback_data.get('temp_dir')
        if temp_dir and Path(temp_dir).exists():
            shutil.rmtree(temp_dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_temp_directory(self, context: UploadContext) -> Path:
        base = context.pathlib_cwd if context.pathlib_cwd else Path(os.getcwd())
        temp_dir = base / 'temp_tiff_images'
        temp_dir.mkdir(parents=True, exist_ok=True)
        return temp_dir

    def _get_tiff_metadata(self, img: Image.Image) -> dict[str, Any]:
        """Extract metadata from a TIFF image."""
        metadata: dict[str, Any] = {
            'image_width': img.width,
            'image_height': img.height,
        }

        if hasattr(img, 'tag'):
            tag_dict = dict(img.tag.named())

            important_tags = {
                'BitsPerSample': 'bits_per_sample',
                'Compression': 'compression',
                'PhotometricInterpretation': 'photometric_interpretation',
                'XResolution': 'x_resolution',
                'YResolution': 'y_resolution',
                'ResolutionUnit': 'resolution_unit',
                'Software': 'software',
                'DateTime': 'datetime',
            }

            for tiff_tag, meta_key in important_tags.items():
                if tiff_tag in tag_dict:
                    metadata[meta_key] = tag_dict[tiff_tag]

        return metadata

    def _count_tiff_frames(self, img: Image.Image) -> int:
        """Count the number of frames in a TIFF file."""
        frame_count = 0
        try:
            while True:
                img.seek(frame_count)
                frame_count += 1
        except EOFError:
            pass

        img.seek(0)
        return frame_count

    def _optimize_image_mode(self, img: Image.Image, output_format: str) -> Image.Image:
        """Optimize image mode for the target output format."""
        if output_format == 'jpg':
            return img.convert('RGB') if img.mode != 'RGB' else img

        if img.mode in ('RGBA', 'LA') or 'transparency' in img.info:
            return img.convert('RGBA') if img.mode != 'RGBA' else img

        return img.convert('RGB') if img.mode != 'RGB' else img

    def _extract_images(
        self,
        tiff_path: Path,
        output_dir: Path,
        output_format: str,
        context: UploadContext,
    ) -> tuple[list[str], dict[str, Any]]:
        """Extract images from a single TIFF file.

        Returns:
            (list of extracted image paths, TIFF metadata dict)
        """
        try:
            file_content = tiff_path.read_bytes()
            if not file_content:
                return [], {}

            file_obj = io.BytesIO(file_content)
            extracted_files: list[str] = []

            with Image.open(file_obj) as img:
                tiff_metadata = self._get_tiff_metadata(img)
                total_frames = self._count_tiff_frames(img)

                if total_frames == 0:
                    return [], tiff_metadata

                ext = f'.{output_format}'
                pil_format = 'JPEG' if output_format == 'jpg' else 'PNG'
                stem = tiff_path.stem

                for i in range(total_frames):
                    try:
                        img.seek(i)
                        current_img = self._optimize_image_mode(img.copy(), output_format)

                        frame_filename = f'{stem}_{i:04d}{ext}'
                        frame_path = output_dir / frame_filename
                        current_img.save(str(frame_path), pil_format)
                        extracted_files.append(str(frame_path))

                        if (i + 1) % 100 == 0:
                            progress = ((i + 1) / total_frames) * 100
                            context.log(
                                'tiff_image_extraction_progress',
                                {'file': tiff_path.name, 'frames': i + 1, 'progress': f'{progress:.1f}%'},
                            )

                    except Exception:
                        continue

            context.log(
                'tiff_images_extracted',
                {'file': tiff_path.name, 'total_images': len(extracted_files)},
            )
            return extracted_files, tiff_metadata

        except Exception:
            return [], {}
