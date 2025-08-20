import os
from huggingface_hub import snapshot_download
from docling.datamodel.pipeline_options import PdfPipelineOptions, RapidOcrOptions
from docling.document_converter import (
    ConversionResult,
    DocumentConverter,
    InputFormat,
    PdfFormatOption,
)
import fitz  # PyMuPDF for extracting images from PDF
from pix2tex.cli import LatexOCR

def extract_images_from_pdf(pdf_path):
    """Extract images from each page of a PDF."""
    pdf = fitz.open(pdf_path)
    images = []
    for i, page in enumerate(pdf, start=1):
        for img in page.get_images(full=True):
            xref = img[0]
            base_image = pdf.extract_image(xref)
            images.append({
                "page": i,
                "bytes": base_image["image"],
                "ext": base_image["ext"],
            })
    return images

def main():
    # Source document to convert
    source = "../Downloads/abctest.pdf"

    # Download RapidOCR models from HuggingFace
    print("Downloading RapidOCR models")
    download_path = snapshot_download(repo_id="SWHL/RapidOCR")

    # Setup RapidOcrOptions for english detection
    det_model_path = os.path.join(
        download_path, "PP-OCRv4", "en_PP-OCRv3_det_infer.onnx"
    )
    rec_model_path = os.path.join(
        download_path, "PP-OCRv4", "ch_PP-OCRv4_rec_server_infer.onnx"
    )
    cls_model_path = os.path.join(
        download_path, "PP-OCRv3", "ch_ppocr_mobile_v2.0_cls_train.onnx"
    )
    ocr_options = RapidOcrOptions(
        det_model_path=det_model_path,
        rec_model_path=rec_model_path,
        cls_model_path=cls_model_path,
    )

    pipeline_options = PdfPipelineOptions(
        ocr_options=ocr_options,
    )

    # Convert the document (text & images)
    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
            ),
        },
    )

    conversion_result: ConversionResult = converter.convert(source=source)
    doc = conversion_result.document
    md = doc.export_to_markdown()

    # Extract images for mathematical formulas
    print("Extracting images from PDF pages for formula detection...")
    images = extract_images_from_pdf(source)
    pix2tex_model = LatexOCR()
    math_md_blocks = []
    for img in images:
        try:
            latex_code = pix2tex_model(img["bytes"])
            # Heuristic: consider result as math if contains a typical math char and length sufficient
            if latex_code and (any(char in latex_code for char in "=\frac{}\sum\sqrt") and len(latex_code.strip()) > 3):
                math_md_blocks.append(
                    f"\n\n**Math Formula from Page {img['page']}**:\n\n$$\n{latex_code}\n$$\n"
                )
        except Exception as e:
            print(f"[WARN] Math OCR failed on page {img['page']}: {e}")

    # Append math formulas to markdown output
    if math_md_blocks:
        md += "\n".join(math_md_blocks)

    # Create the output filename based on the input filename
    input_filename = os.path.basename(source)
    input_filename_without_extension = os.path.splitext(input_filename)[0]
    output_filename = f"new{input_filename_without_extension}.md"

    # Create the 'output' directory if it doesn't exist
    output_directory = "output"
    os.makedirs(output_directory, exist_ok=True)
    output_path = os.path.join(output_directory, output_filename)

    # Write the Markdown output to the file
    with open(output_path, 'w', encoding='utf-8') as outfile:
        outfile.write(md)

    print(f"Markdown output written to: {output_path}")

if __name__ == "__main__":
    main()
