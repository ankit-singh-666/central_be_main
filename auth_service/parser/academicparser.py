import os
import hashlib
import traceback
from functools import partial
from pathlib import Path
from tqdm import tqdm
from PIL import Image
import torch
import pypdfium2

from nougat import NougatModel
from nougat.postprocessing import markdown_compatible, close_envs
from nougat.utils.dataset import ImageDataset
from nougat.utils.checkpoint import get_checkpoint
from nougat.dataset.rasterize import rasterize_paper
from nougat.utils.device import move_to_device, default_batch_size

from huggingface_hub import snapshot_download
from docling.datamodel.pipeline_options import PdfPipelineOptions, RapidOcrOptions
from docling.document_converter import (
    ConversionResult,
    DocumentConverter,
    InputFormat,
    PdfFormatOption,
)

# ------------------ SETTINGS ------------------
PDF_PATH = "/kaggle/input/v5docs/abctest.pdf"  # Input PDF
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
BATCHSIZE = int(os.environ.get("NOUGAT_BATCHSIZE", default_batch_size()))

# Set this flag: True = use Nougat (math formulas), False = only Docling
academia_check = True

# ------------------ DOC OUTPUT PATH ------------------
pdf_filename = os.path.basename(PDF_PATH)
pdf_stem = os.path.splitext(pdf_filename)[0]
output_md_path = OUTPUT_DIR / f"{pdf_stem}.md"

# ------------------ DOC OUTPUT INIT ------------------
final_markdown = ""

# ------------------ ACADEMIA CHECK: NOUGAT ------------------
if academia_check:
    print("⚡ Running Nougat (math formulas) extraction...")
    try:
        # Load Nougat checkpoint
        NOUGAT_CHECKPOINT = get_checkpoint("facebook/nougat-base")
        if NOUGAT_CHECKPOINT is None:
            raise ValueError("Set NOUGAT_CHECKPOINT environment variable or use get_checkpoint()!")

        device = "mps" if torch.backends.mps.is_available() else ("cuda" if torch.cuda.is_available() else "cpu")
        print(f"Using device: {device}")

        model = NougatModel.from_pretrained(NOUGAT_CHECKPOINT)
        model = move_to_device(model, cuda=BATCHSIZE > 0)
        model.eval()
    except Exception:
        print("❌ Error loading Nougat model:")
        traceback.print_exc()
        raise

    # Load PDF
    try:
        with open(PDF_PATH, "rb") as f:
            pdfbin = f.read()
        pdf = pypdfium2.PdfDocument(pdfbin)
        md5 = hashlib.md5(pdfbin).hexdigest()
        save_path = OUTPUT_DIR / md5
        (save_path / "pages").mkdir(parents=True, exist_ok=True)
    except Exception:
        print("❌ Error reading PDF:")
        traceback.print_exc()
        raise

    # Rasterize PDF pages
    try:
        pages = list(range(len(pdf)))
        images = rasterize_paper(pdf, pages=pages)
    except Exception:
        print("❌ Error rasterizing PDF:")
        traceback.print_exc()
        raise

    # Prepare Dataset
    try:
        dataset = ImageDataset(images, partial(model.encoder.prepare_input, random_padding=False))
        dataloader = torch.utils.data.DataLoader(dataset, batch_size=BATCHSIZE, shuffle=False, pin_memory=True)
    except Exception:
        print("❌ Error preparing ImageDataset:")
        traceback.print_exc()
        raise

    # Run Nougat
    predictions = [""] * len(pages)
    try:
        for idx, sample in tqdm(enumerate(dataloader), total=len(dataloader)):
            if sample is None:
                continue
            model_output = model.inference(image_tensors=sample)
            for j, output in enumerate(model_output["predictions"]):
                page_idx = idx * BATCHSIZE + j
                if page_idx >= len(pages):
                    continue
                if model_output["repeats"][j] is not None:
                    if model_output["repeats"][j] > 0:
                        disclaimer = "\n\n+++ ==WARNING: Truncated because of repetitions==\n%s\n+++\n\n"
                    else:
                        disclaimer = "\n\n+++ ==ERROR: No output for this page==\n%s\n+++\n\n"
                    rest = close_envs(model_output["repetitions"][j]).strip()
                    if len(rest) > 0:
                        disclaimer = disclaimer % rest
                    else:
                        disclaimer = ""
                else:
                    disclaimer = ""
                predictions[page_idx] = markdown_compatible(output) + disclaimer
        final_markdown += "## Nougat (math + formulas)\n\n" + "\n\n".join(predictions) + "\n\n"
    except Exception:
        print("❌ Error during Nougat inference:")
        traceback.print_exc()
        raise

# ------------------ DOC OUTPUT: DOCLING ------------------
print("📥 Running Docling (text/tables extraction)...")
try:
    # Download RapidOCR models from HuggingFace
    download_path = snapshot_download(repo_id="SWHL/RapidOCR")
    det_model_path = os.path.join(download_path, "PP-OCRv4", "en_PP-OCRv3_det_infer.onnx")
    rec_model_path = os.path.join(download_path, "PP-OCRv4", "ch_PP-OCRv4_rec_server_infer.onnx")
    cls_model_path = os.path.join(download_path, "PP-OCRv3", "ch_ppocr_mobile_v2.0_cls_train.onnx")

    ocr_options = RapidOcrOptions(
        det_model_path=det_model_path,
        rec_model_path=rec_model_path,
        cls_model_path=cls_model_path,
    )
    pipeline_options = PdfPipelineOptions(ocr_options=ocr_options)

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
            ),
        },
    )
    conversion_result: ConversionResult = converter.convert(source=PDF_PATH)
    doc = conversion_result.document
    docling_md = doc.export_to_markdown()
    final_markdown = "## Docling (text + tables)\n\n" + docling_md + "\n\n" + final_markdown
except Exception:
    print("❌ Error during Docling conversion:")
    traceback.print_exc()
    raise

# ------------------ SAVE FINAL MARKDOWN ------------------
try:
    with open(output_md_path, "w", encoding="utf-8") as f:
        f.write(final_markdown)
    print(f"✅ Final Markdown saved to: {output_md_path}")
except Exception:
    print("❌ Error saving final Markdown:")
    traceback.print_exc()
    raise
