import os
import re
import io
import tempfile
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from llama_index.core import SimpleDirectoryReader, Settings, VectorStoreIndex , StorageContext
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.vector_stores.chroma import ChromaVectorStore
import config
import chromadb
from parser import process_document_enhanced
from llama_index.core.vector_stores.types import (
    MetadataFilter, MetadataFilters, FilterOperator, FilterCondition
)
from typing import Union, List

class GoogleDriveIndexer:
    """
    Complete Google Drive Indexer with:
    1. Folder or file URL processing
    2. Auto-detection of input type
    3. File filtering for supported formats
    4. Document enrichment
    5. Persistent embedding store (ChromaDB + OpenAIEmbedding)
    """

    def __init__(
        self,
        service_account_path: str = "service_key.json",
        chroma_path: str = "./embeddings_storage/chroma_db",
        embedding_model: str = "text-embedding-3-small",
        openai_api_key: str = config.OPENAI_API_KEY,
    ):
        self.service_account_path = service_account_path
        self.chroma_path = chroma_path
        self.embedding_model = embedding_model
        self.index = None
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")

        if not self.openai_api_key:
            raise ValueError(
                "OpenAI API key is required. Set it via openai_api_key or environment variable."
            )

        # File formats supported
        self.supported_formats = [".pdf", ".docx"]

        # Setup embeddings + persistent Chroma
        self._setup_embedding()

    def _setup_embedding(self):
        """Configure embedding model and persistent Chroma vector store."""
        # Setup OpenAI embeddings
        Settings.embed_model = OpenAIEmbedding(
            model=self.embedding_model, api_key=self.openai_api_key
        )
        Settings.chunk_size = 512
        Settings.chunk_overlap = 50

        print(
            f"✅ Embedding model '{self.embedding_model}' set up with Chroma persistence at {self.chroma_path}"
        )

    def build_index_from_drive(
        self, drive_input, collection_name: str = "drive_docs", input_type: str = "auto"
    ):
        """
        Build search index from Google Drive

        Args:
            drive_input: Either folder URL or list of file URLs
            collection_name: Name for the document collection
            input_type: "folder", "files", or "auto"
        """
        # Load service account credentials
        creds = service_account.Credentials.from_service_account_file(
            self.service_account_path
        )
        drive_service = build("drive", "v3", credentials=creds)

        files_to_process = []

        # Auto-detect input type
        if input_type == "auto":
            input_type = self._detect_input_type(drive_input)

        # Collect files
        if input_type == "folder":
            files_to_process = self._get_files_from_folder(drive_service, drive_input)
            print(f"📂 Processing entire folder: {len(files_to_process)} files found")

        elif input_type == "files":
            files_to_process = self._get_files_from_urls(drive_service, drive_input)
            print(f"📄 Processing {len(files_to_process)} specific files")

        else:
            raise ValueError("input_type must be 'folder', 'files', or 'auto'")

        if not files_to_process:
            raise ValueError("No files found to process")

        # Process all collected files
        processing_summary = self._process_drive_files(drive_service, files_to_process)
        enriched_docs = processing_summary["documents"]

        if not enriched_docs:
            raise ValueError("No documents ready after enrichment")

        # Create the index
        self.index = self._create_index(enriched_docs, collection_name)

        # Update summary
        processing_summary["index_created"] = True
        processing_summary["collection_name"] = collection_name
        

        print("✅ Drive index built (with enrichment) and ready.")
        return processing_summary

    def _detect_input_type(self, drive_input):
        if isinstance(drive_input, list):
            return "files"
        elif isinstance(drive_input, str):
            if "/folders/" in drive_input:
                return "folder"
            elif "/file/d/" in drive_input or "id=" in drive_input:
                return "files"

        raise ValueError(
            "Could not auto-detect input type. Please specify 'folder' or 'files'."
        )

    def _get_files_from_folder(self, drive_service, folder_link: str):
        match = re.search(r"/folders/([a-zA-Z0-9_-]+)", folder_link)
        if not match:
            raise ValueError("Invalid Google Drive folder link format.")
        folder_id = match.group(1)

        print(f"📥 Loading files from Drive folder {folder_id}...")

        try:
            results = (
                drive_service.files()
                .list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    fields="files(id, name, mimeType, size, modifiedTime)",
                )
                .execute()
            )
            files = results.get("files", [])
            print(f"✅ Found {len(files)} files in folder")
            return files

        except Exception as e:
            raise ValueError(f"Error accessing folder {folder_id}: {str(e)}")

    def _get_files_from_urls(self, drive_service, file_input):
        file_urls = [file_input] if isinstance(file_input, str) else file_input
        files = []
        print(f"📥 Processing {len(file_urls)} file URL(s)...")

        for url in file_urls:
            file_id = self._extract_file_id(url)
            if not file_id:
                print(f"⚠️ Could not extract file ID from: {url}")
                continue
            try:
                file_info = (
                    drive_service.files()
                    .get(fileId=file_id, fields="id, name, mimeType, size, modifiedTime")
                    .execute()
                )
                files.append(file_info)
                print(f"✅ Added file: {file_info['name']}")
            except Exception as e:
                print(f"⚠️ Error accessing file {file_id}: {e}")
                continue
        return files

    def _extract_file_id(self, url):
        match = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
        if match:
            return match.group(1)
        match = re.search(r"id=([a-zA-Z0-9_-]+)", url)
        if match:
            return match.group(1)
        if re.match(r"^[a-zA-Z0-9_-]+$", url.strip()):
            return url.strip()
        return None

    def _process_drive_files(self, drive_service, files: list):
        enriched_docs = []
        processing_stats = {
            "total_files": len(files),
            "processed": 0,
            "skipped": 0,
            "errors": 0,
            "skipped_reasons": [],
            "error_details": [],
            "processed_files": [],
            "documents": [],
        }

        for f in files:
            file_id = f["id"]
            file_name = f["name"]
            file_ext = Path(file_name).suffix.lower()

            if file_ext not in self.supported_formats:
                print(f"⏭️ Skipping {file_name} (unsupported format: {file_ext})")
                processing_stats["skipped"] += 1
                processing_stats["skipped_reasons"].append(
                    f"{file_name}: unsupported format ({file_ext})"
                )
                continue

            try:
                print(f"⬇️ Downloading {file_name}...")
                request = drive_service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        progress = int(status.progress() * 100)
                        print(f"   📊 Download progress: {progress}%", end="\r")

                # Save to temp
                temp_input = Path(tempfile.gettempdir()) / file_name
                with open(temp_input, "wb") as tmpf:
                    tmpf.write(fh.getvalue())

                print(f"\n✨ Enriching {file_name} with process_document_enhanced...")

                # ⚠️ NOTE: You need to implement this function
                enriched_md_path = process_document_enhanced(str(temp_input))

                # Load enriched docs
                md_docs = SimpleDirectoryReader(
                    input_files=[enriched_md_path]
                ).load_data()

                for d in md_docs:
                    d.metadata.update(
                        {
                            "file_name": file_name,
                            "file_type": file_ext,
                            "file_id": file_id,
                            "source": "google_drive",
                            "file_size": f.get("size", "unknown"),
                            "modified_time": f.get("modifiedTime", "unknown"),
                        }
                    )

                enriched_docs.extend(md_docs)
                processing_stats["processed"] += 1
                processing_stats["processed_files"].append(file_name)

                temp_input.unlink(missing_ok=True)
                print(f"✅ Successfully processed {file_name}")

            except Exception as e:
                error_msg = f"Error processing {file_name}: {str(e)}"
                print(f"⚠️ {error_msg}")
                processing_stats["errors"] += 1
                processing_stats["error_details"].append(error_msg)

        processing_stats["documents"] = enriched_docs
        print(
            f"\n📊 Summary: {processing_stats['processed']} processed, {processing_stats['skipped']} skipped, {processing_stats['errors']} errors"
        )
        return processing_stats

    def _create_index(self, docs, collection_name):
        # ✅ Local persistent client
        chroma_client = chromadb.PersistentClient(path=self.chroma_path)

        # ✅ Wrap the existing Chroma collection with llama-index's ChromaVectorStore
        vector_store = ChromaVectorStore(
            chroma_collection=chroma_client.get_or_create_collection(collection_name)
        )

        # ✅ Create llama-index storage context on top of that vector store
        storage_context = StorageContext.from_defaults(vector_store=vector_store)

        # ✅ Build a VectorStoreIndex with progress
        return VectorStoreIndex.from_documents(
            docs,
            storage_context=storage_context,
            show_progress=True
        )


    def query_index(self, query_text: str, k: int = 3, source: str = None, file_type: str = None):
        """Query the built index with optional filters."""
        if not self.index:
            raise RuntimeError("❌ No index built yet. Use one of the build_index_* methods first.")

        # -------------------------------
        # Build metadata filters
        # -------------------------------
        filters_list = []
        if source:
            filters_list.append(
                MetadataFilter(key="source", value=source, operator=FilterOperator.EQ)
            )
        if file_type:
            filters_list.append(
                MetadataFilter(key="file_type", value=file_type, operator=FilterOperator.EQ)
            )

        metadata_filters = (
            MetadataFilters(filters=filters_list, condition=FilterCondition.AND)
            if filters_list else None
        )

        # -------------------------------
        # Retriever setup
        # -------------------------------
        retriever = self.index.as_retriever(
            similarity_top_k=k,
            filters=metadata_filters
        )

        # -------------------------------
        # Retrieve results
        # -------------------------------
        results = retriever.retrieve(query_text)

        context_texts = []
        for r in results:
            fname = r.metadata.get("file_name", "Unknown")
            chunk = r.node.get_content()
            context_texts.append(f"[📄 File: {fname}]\n{chunk}")

        return "\n\n".join(context_texts)

    def get_index_stats(self):
        if not self.index:
            return {"status": "No index created"}
        try:
            docstore = self.index.docstore
            return {
                "status": "Index ready",
                "document_count": len(docstore.docs),
                "supported_formats": self.supported_formats,
            }
        except:
            return {"status": "Index created but stats unavailable"}

    def get_drive_folder_files(self, drive_links: Union[str, List[str]]):
        """Preview one or multiple Google Drive folders/files without building index."""

        # ✅ Always normalize input to a list
        if isinstance(drive_links, str):
            drive_links = [drive_links]

        creds = service_account.Credentials.from_service_account_file(
            self.service_account_path
        )
        drive_service = build("drive", "v3", credentials=creds)

        all_files = []

        for link in drive_links:
            files = self._get_files_from_folder(drive_service, link)

            for f in files:
                file_ext = Path(f["name"]).suffix.lower()
                all_files.append(
                    {
                        "id": f["id"],
                        "name": f["name"],
                        "type": file_ext,
                        "size": f.get("size", "Unknown"),
                        "modified": f.get("modifiedTime", "Unknown"),
                        "supported": file_ext in self.supported_formats,
                        "url": f"https://drive.google.com/file/d/{f['id']}/view",
                    }
                )

        return all_files


if __name__ == "__main__":
    indexer = GoogleDriveIndexer()
    print("Google Drive Indexer initialized. Ready to build indexes or query.")