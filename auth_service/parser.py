import re
import openai
from docling.document_converter import DocumentConverter
import tempfile
from pathlib import Path
import config

# -----------------------------
# CONFIG
# -----------------------------
OPENAI_MODEL = config.OPENAI_MODELS["chat"]
openai.api_key = config.OPENAI_API_KEY
api_call_count = 0

print("This is parser.py ==================> ",openai.api_key)  # Debug: print API key to ensure it's loaded correctly

# -----------------------------
# ENHANCED FORMULA & IMAGE EXTRACTION
# -----------------------------
def extract_elements_by_page(data):
    """Extract formulas, images, and other elements by page using document structure"""
    formulas_by_page = {}
    images_by_page = {}
    figures_by_page = {}
    
    print("🔍 Analyzing document structure...")
    
    # Navigate through the document structure
    for ref in data.get("body", {}).get("children", []):
        if "$ref" not in ref:
            continue
            
        # Follow the reference path
        node = data
        try:
            for key in ref["$ref"].split("/")[1:]:
                if key.isdigit():
                    node = node[int(key)]
                else:
                    node = node.get(key, {})
        except (KeyError, IndexError, ValueError):
            continue
        
        if not isinstance(node, dict):
            continue
            
        label = node.get("label", "")
        page_no = 1  # default page
        
        # Extract page number from provenance
        prov = node.get("prov", [])
        if prov and isinstance(prov, list) and len(prov) > 0:
            page_no = prov[0].get("page_no", 1)
        
        # Handle formulas
        if label == "formula":
            formula_str = node.get("text", "").strip() or node.get("orig", "").strip()
            if formula_str:
                formulas_by_page.setdefault(page_no, []).append(f"$${formula_str}$$")
                print(f"   📐 Formula found on page {page_no}: {formula_str[:50]}...")
        
        # Handle images/pictures
        elif label in ["picture", "image"]:
            image_info = {
                "caption": node.get("caption", node.get("text", "Image")),
                "type": label
            }
            images_by_page.setdefault(page_no, []).append(image_info)
            print(f"   🖼️ Image found on page {page_no}: {image_info['caption'][:50]}...")
        
        # Handle figures
        elif label == "figure":
            figure_info = {
                "caption": node.get("caption", node.get("text", "Figure")),
                "type": "figure"
            }
            figures_by_page.setdefault(page_no, []).append(figure_info)
            print(f"   📊 Figure found on page {page_no}: {figure_info['caption'][:50]}...")
    
    print(f"📊 Extraction complete:")
    print(f"   Formulas on {len(formulas_by_page)} pages")
    print(f"   Images on {len(images_by_page)} pages") 
    print(f"   Figures on {len(figures_by_page)} pages")
    
    return formulas_by_page, images_by_page, figures_by_page

# -----------------------------
# ENHANCED OPENAI PROCESSING
# -----------------------------
def ask_openai_for_page(page_text, page_num, formulas=None, images=None, figures=None):
    """Enhanced OpenAI processing with better context"""
    global api_call_count
    api_call_count += 1
    
    print(f"🤖 API Call #{api_call_count} - Processing page {page_num}")
    
    # Prepare context information
    context_info = []
    
    if formulas:
        formulas_str = "\n".join(formulas)
        context_info.append(f"FORMULAS ON THIS PAGE:\n{formulas_str}")
    
    if images:
        images_str = "\n".join([f"- {img['type'].title()}: {img['caption']}" for img in images])
        context_info.append(f"IMAGES ON THIS PAGE:\n{images_str}")
        
    if figures:
        figures_str = "\n".join([f"- {fig['type'].title()}: {fig['caption']}" for fig in figures])
        context_info.append(f"FIGURES ON THIS PAGE:\n{figures_str}")
    
    context = "\n\n".join(context_info) if context_info else "No special elements detected"
    
    prompt = f"""
You are processing page {page_num} of a neural networks textbook/document.

CONTEXT INFORMATION:
{context}

PAGE CONTENT:
{page_text}

TASK - Generate clean, complete Markdown for this page:

1. **FORMULAS**: Replace any "<!-- formula-not-decoded -->" or similar placeholders with proper LaTeX
   - Use the formulas provided in context above
   - Place them appropriately in the text flow
   - Use $formula$ for inline math, $$formula$$ for display math

2. **IMAGES**: Replace "<!-- image -->" placeholders with descriptive markdown images
   - Use information from context above
   - Format: ![descriptive caption] "Add the summary/table/text what ever you want"
   - Create meaningful descriptions based on surrounding text

3. **CONTENT QUALITY**:
   - Fix any OCR errors or formatting issues
   - Maintain academic tone and technical accuracy
   - Keep all information intact
   - Ensure proper markdown structure

4. **SPECIAL HANDLING**:
   - For neural network content, ensure mathematical notation is correct
   - Preserve any code snippets or technical specifications
   - Maintain proper heading hierarchy

Return the COMPLETE page content in clean Markdown format.
Note that don't add any extra line from your side like "This Markdown format maintains the structure and content of the original page while ensuring clarity and proper formatting. Descriptive captions for images have been added, and the content has been organized into appropriate sections and headings."
"""
    
    try:
        resp = openai.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=4000
        )
        
        result = resp.choices[0].message.content.strip()
        print(f"✅ Page {page_num} processed successfully")
        return result
        
    except Exception as e:
        print(f"❌ API error for page {page_num}: {e}")
        return page_text  # Return original content if API fails

# -----------------------------
# IMPROVED PAGE DETECTION & PROCESSING
# -----------------------------
def process_document_enhanced(source_path:str):
    """Enhanced document processing with better page detection"""
    print("🚀 Starting enhanced document processing...")
    
    # 1) Parse with Docling
    converter = DocumentConverter()
    result = converter.convert(source_path)
    
    markdown_doc = result.document.export_to_markdown()
    data = result.document.export_to_dict()
    
    print(f"📄 Document loaded: {len(markdown_doc)} characters")
    
    # 2) Extract elements by page
    formulas_by_page, images_by_page, figures_by_page = extract_elements_by_page(data)
    
    # 3) Detect page structure
    lines = markdown_doc.split("\n")
    
    # Check different page detection methods
    page_patterns = [
        r"^Page (\d+)",           # "Page 1", "Page 2", etc.
        r"^# Page (\d+)",         # "# Page 1", "# Page 2", etc.  
        r"^## Page (\d+)",        # "## Page 1", "## Page 2", etc.
        r"^\[Page (\d+)\]",       # "[Page 1]", "[Page 2]", etc.
    ]
    
    page_breaks = []
    for i, line in enumerate(lines):
        for pattern in page_patterns:
            match = re.match(pattern, line.strip())
            if match:
                page_num = int(match.group(1))
                page_breaks.append((i, page_num, line))
                break
    
    print(f"📄 Found {len(page_breaks)} page breaks")
    
    # 4) Process pages
    final_lines = []
    pages_processed = 0
    
    if page_breaks:
        # Process page by page
        for i, (line_idx, page_num, page_header) in enumerate(page_breaks):
            # Add page header
            final_lines.append(page_header)
            
            # Get page content (from this page break to next, or end)
            start_idx = line_idx + 1
            if i + 1 < len(page_breaks):
                end_idx = page_breaks[i + 1][0]
            else:
                end_idx = len(lines)
            
            page_lines = lines[start_idx:end_idx]
            page_text = "\n".join(page_lines)
            
            # Check if page needs processing
            has_image = "<!-- image -->" in page_text
            has_formula_placeholder = "<!-- formula" in page_text  
            has_extracted_elements = (
                page_num in formulas_by_page or 
                page_num in images_by_page or 
                page_num in figures_by_page
            )
            
            should_process = has_image or has_formula_placeholder or has_extracted_elements
            
            print(f"📄 Page {page_num}: images={has_image}, formulas={has_formula_placeholder}, elements={has_extracted_elements}, process={should_process}")
            
            if should_process:
                enriched = ask_openai_for_page(
                    page_text, 
                    page_num,
                    formulas=formulas_by_page.get(page_num, []),
                    images=images_by_page.get(page_num, []),
                    figures=figures_by_page.get(page_num, [])
                )
                final_lines.extend(enriched.split("\n"))
                pages_processed += 1
            else:
                final_lines.extend(page_lines)
                print(f"⏭️ Skipped page {page_num} (no elements to process)")
    
    else:
        # No clear page breaks - split by content size
        print("📄 No page breaks found - splitting by content chunks")
        
        chunk_size = 3000
        words = markdown_doc.split()
        
        chunk_num = 1
        current_chunk = []
        current_size = 0
        
        for word in words:
            current_chunk.append(word)
            current_size += len(word) + 1
            
            if current_size >= chunk_size:
                chunk_text = " ".join(current_chunk)
                
                # Check if chunk needs processing
                has_placeholders = ("<!-- image -->" in chunk_text or 
                                  "<!-- formula" in chunk_text)
                
                if has_placeholders:
                    enriched = ask_openai_for_page(chunk_text, chunk_num, [], [], [])
                    final_lines.extend(enriched.split("\n"))
                    pages_processed += 1
                else:
                    final_lines.extend(chunk_text.split("\n"))
                
                final_lines.append("---")  # Separator between chunks
                current_chunk = []
                current_size = 0
                chunk_num += 1
        
        # Handle remaining chunk
        if current_chunk:
            chunk_text = " ".join(current_chunk)
            has_placeholders = ("<!-- image -->" in chunk_text or 
                              "<!-- formula" in chunk_text)
            
            if has_placeholders:
                enriched = ask_openai_for_page(chunk_text, chunk_num, [], [], [])
                final_lines.extend(enriched.split("\n"))
                pages_processed += 1
            else:
                final_lines.extend(chunk_text.split("\n"))
    
    # 5) Generate final output
    final_markdown = "\n".join(final_lines)
    
    print(f"\n📊 PROCESSING SUMMARY:")
    print(f"   Total API calls: {api_call_count}")
    print(f"   Pages/chunks processed: {pages_processed}")
    print(f"   Final document length: {len(final_markdown)} characters")
    # Instead of returning text, save and return path
    
    md_path = Path(tempfile.gettempdir()) / f"{Path(source_path).stem}_enhanced.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(final_markdown)

    return str(md_path)   # ✅ return file path

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    try:
        final_result = process_document_enhanced("sample_document.pdf")
        
        # Save output
        with open("abc_pitch.md", "w", encoding="utf-8") as f:
            f.write(final_result)
        
        print("✅ Processing complete! Output saved to 'output_enhanced.md'")
        
        # Show sample output
        print(f"\n📝 SAMPLE OUTPUT (first 800 chars):")
        print("-" * 60)
        print(final_result[:800] + "...")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()