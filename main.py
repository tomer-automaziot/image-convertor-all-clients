import os
import re
import io
import json
import httpx
import tempfile
import subprocess
from fastapi import FastAPI, Request, Response
from PIL import Image
from supabase import create_client

app = FastAPI()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
BUCKET = os.environ.get("BUCKET_NAME", "product-images")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def sku_to_folder(sku: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "", sku)


def is_supabase_url(url: str) -> bool:
    return "supabase.co/storage/" in url


def convert_image(raw_bytes: bytes, source_url: str, content_type: str) -> tuple[bytes, str, str]:
    """Convert any image to 8-bit JPEG. Returns (bytes, extension, mime_type)."""
    img = Image.open(io.BytesIO(raw_bytes))

    # Handle high bit-depth modes (10-bit/16-bit) → 8-bit
    if img.mode in ("I;16", "I;16L", "I;16B", "I;16N"):
        import numpy as np
        arr = np.array(img, dtype=np.uint16)
        arr = (arr >> 8).astype(np.uint8)
        img = Image.fromarray(arr, mode="L")
    elif img.mode in ("I", "F"):
        img = img.convert("L")

    # Handle transparency → composite on white background
    if img.mode in ("RGBA", "P", "LA", "PA"):
        if img.mode in ("P", "PA"):
            img = img.convert("RGBA")
        background = Image.new("RGB", img.size, (255, 255, 255))
        background.paste(img, mask=img.split()[-1])
        img = background
    elif img.mode != "RGB":
        img = img.convert("RGB")

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    return buf.getvalue(), ".jpeg", "image/jpeg"


def convert_video(raw_bytes: bytes) -> bytes:
    """
    Convert video to WhatsApp-compatible MP4.
    Input: any MP4 (including H.264 High 10 Profile 10-bit)
    Output: H.264 High Profile 8-bit, AAC audio, faststart
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input.mp4")
        output_path = os.path.join(tmpdir, "output.mp4")

        # Write input bytes to temp file
        with open(input_path, "wb") as f:
            f.write(raw_bytes)

        # Run FFmpeg conversion
        cmd = [
            "ffmpeg",
            "-i", input_path,
            "-c:v", "libx264",
            "-profile:v", "high",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-movflags", "+faststart",
            "-y",  # overwrite output if exists
            output_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"FFmpeg failed: {result.stderr}")

        with open(output_path, "rb") as f:
            return f.read()


async def download_file(client: httpx.AsyncClient, url: str) -> tuple[bytes, str]:
    """Download file and return (bytes, content_type)."""
    resp = await client.get(url, follow_redirects=True, timeout=60.0)
    resp.raise_for_status()
    return resp.content, resp.headers.get("content-type", "")


async def download_image(client: httpx.AsyncClient, url: str) -> tuple[bytes, str]:
    """Download image and return (bytes, content_type)."""
    return await download_file(client, url)


def upload_image(path: str, data: bytes, content_type: str) -> str:
    """Upload to Supabase storage and return public URL."""
    supabase.storage.from_(BUCKET).upload(
        path, data,
        file_options={"content-type": content_type, "upsert": "true", "cache-control": "3600"}
    )
    res = supabase.storage.from_(BUCKET).get_public_url(path)
    return res.rstrip("?")


def list_folder_files(folder: str) -> list[str]:
    """List all files in a storage folder."""
    try:
        files = supabase.storage.from_(BUCKET).list(folder, {"limit": 1000})
        return [f"{folder}/{f['name']}" for f in files if f.get("id")]
    except Exception:
        return []


def delete_files(paths: list[str]):
    """Delete files from storage."""
    if paths:
        supabase.storage.from_(BUCKET).remove(paths)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/process-project-upload")
async def process_project_upload(request: Request):
    """
    Process a file uploaded to Supabase storage for a project.
    Called directly by Supabase webhook on storage.objects INSERT.

    Supabase webhook payload contains:
    {
        "record": {
            "bucket_id": "media",
            "name": "project-uuid/filename.jpg",   # folder = project id
            "metadata": { "mimetype": "image/jpeg" }
        }
    }

    Flow:
    1. Parse project_id and filename from the storage path
    2. Download the uploaded file
    3. Convert image to JPEG or video to MP4
    4. Upload converted file back to bucket (replace original)
    5. Update photos[] or videos[] array in projects table
    """
    if WEBHOOK_SECRET and request.headers.get("x-webhook-secret") != WEBHOOK_SECRET:
        return Response(status_code=401, content="Unauthorized")

    body = await request.json()
    record = body.get("record") or body

    # Parse path: "project-uuid/filename.ext"
    name = record.get("name", "")
    bucket_id = record.get("bucket_id", BUCKET)
    mime_type = ""
    metadata = record.get("metadata") or {}
    if isinstance(metadata, dict):
        mime_type = metadata.get("mimetype") or metadata.get("contentType") or ""

    parts = name.split("/")
    if len(parts) < 2:
        return {"success": False, "error": "Invalid path format. Expected: project_id/filename"}

    project_id = parts[0]
    filename = "/".join(parts[1:])
    base_name = filename.rsplit(".", 1)[0]

    is_image = mime_type.startswith("image/")
    is_video = mime_type.startswith("video/")

    if not is_image and not is_video:
        return {"success": True, "message": "File is not an image or video, skipped", "path": name}

    # Build public URL of uploaded file
    public_url = f"{SUPABASE_URL}/storage/v1/object/public/{bucket_id}/{name}"

    try:
        async with httpx.AsyncClient() as client:
            raw_bytes, content_type = await download_file(client, public_url)

        if is_image:
            converted, ext, mime = convert_image(raw_bytes, public_url, content_type)
            storage_path = f"{project_id}/{base_name}{ext}"
            final_url = upload_image(storage_path, converted, mime)
            col = "photos"
        else:
            converted = convert_video(raw_bytes)
            storage_path = f"{project_id}/{base_name}.mp4"
            final_url = upload_image(storage_path, converted, "video/mp4")
            col = "videos"

        # Fetch current array from projects table
        resp = supabase.table("projects").select(col).eq("id", project_id).single().execute()
        if not resp.data:
            return {"success": False, "error": f"Project {project_id} not found in projects table"}

        current = resp.data.get(col) or []

        # Replace old URL or append new one
        old_url = f"{SUPABASE_URL}/storage/v1/object/public/{bucket_id}/{name}"
        if old_url in current:
            updated = [final_url if u == old_url else u for u in current]
        elif final_url not in current:
            updated = current + [final_url]
        else:
            updated = current

        supabase.table("projects").update({col: updated}).eq("id", project_id).execute()

        return {
            "success": True,
            "project_id": project_id,
            "column": col,
            "original_path": name,
            "converted_path": storage_path,
            "public_url": final_url
        }

    except Exception as e:
        return {"success": False, "error": str(e), "path": name}


@app.post("/convert-video")
async def convert_video_endpoint(request: Request):
    """
    Convert a video to WhatsApp-compatible MP4 and upload to Supabase storage.

    Request body:
    {
        "video_url": "https://...",   # URL of the source video
        "sku": "PRODUCT_SKU",         # Used to build the storage path
        "filename": "video_1"         # Optional, defaults to "video_1"
    }

    Returns:
    {
        "success": true,
        "public_url": "https://..."
    }
    """
    if WEBHOOK_SECRET and request.headers.get("x-webhook-secret") != WEBHOOK_SECRET:
        return Response(status_code=401, content="Unauthorized")

    payload = await request.json()
    video_url = payload.get("video_url")
    sku = payload.get("sku")
    filename = payload.get("filename", "video_1")

    if not video_url:
        return {"error": "video_url is required"}
    if not sku:
        return {"error": "sku is required"}

    folder = sku_to_folder(sku)
    storage_path = f"{folder}/{filename}.mp4"

    try:
        async with httpx.AsyncClient() as client:
            raw_bytes, content_type = await download_file(client, video_url)

        converted = convert_video(raw_bytes)
        public_url = upload_image(storage_path, converted, "video/mp4")

        return {"success": True, "public_url": public_url, "path": storage_path}

    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/sync-product-images")
async def sync_product_images(request: Request):
    # Validate webhook secret
    if WEBHOOK_SECRET and request.headers.get("x-webhook-secret") != WEBHOOK_SECRET:
        return Response(status_code=401, content="Unauthorized")

    payload = await request.json()
    sku = payload.get("sku")
    product_images = payload.get("product_images") or []
    showroom_images = payload.get("showroom_images") or []

    if not sku:
        return {"error": "SKU is required"}

    folder = sku_to_folder(sku)
    results = {"uploaded": [], "deleted": [], "skipped": [], "errors": []}

    # Collect items to process
    items = []
    for i, url in enumerate(product_images):
        if url and not is_supabase_url(url):
            items.append({"url": url, "base": f"{folder}/product_{i+1}", "type": "product"})
        elif url and is_supabase_url(url):
            results["skipped"].append(url)

    for i, url in enumerate(showroom_images):
        if url and not is_supabase_url(url):
            items.append({"url": url, "base": f"{folder}/showroom_{i+1}", "type": "showroom"})
        elif url and is_supabase_url(url):
            results["skipped"].append(url)

    if not items:
        return {"success": True, "message": "No new images to sync", "results": results}

    # Download, convert, upload
    new_product_urls = []
    new_showroom_urls = []
    uploaded_paths = set()

    async with httpx.AsyncClient() as client:
        for item in items:
            try:
                raw_bytes, content_type = await download_image(client, item["url"])
                converted, ext, mime = convert_image(raw_bytes, item["url"], content_type)
                full_path = item["base"] + ext
                public_url = upload_image(full_path, converted, mime)

                results["uploaded"].append(full_path)
                uploaded_paths.add(full_path)

                if item["type"] == "product":
                    new_product_urls.append(public_url)
                else:
                    new_showroom_urls.append(public_url)
            except Exception as e:
                results["errors"].append(f"{item['base']}: {str(e)}")

    # Clean up orphaned files
    existing = list_folder_files(folder)
    to_delete = []
    for path in existing:
        if path not in uploaded_paths:
            is_kept = any(path in url for url in results["skipped"])
            if not is_kept:
                to_delete.append(path)
    if to_delete:
        delete_files(to_delete)
        results["deleted"].extend(to_delete)

    # Update inventory record
    update_data = {}
    if new_product_urls:
        final = []
        url_iter = iter(new_product_urls)
        for url in product_images:
            if is_supabase_url(url):
                final.append(url)
            else:
                final.append(next(url_iter, url))
        update_data["product_images"] = final

    if new_showroom_urls:
        final = []
        url_iter = iter(new_showroom_urls)
        for url in showroom_images:
            if is_supabase_url(url):
                final.append(url)
            else:
                final.append(next(url_iter, url))
        update_data["showroom_images"] = final

    if update_data:
        try:
            supabase.table("inventory").update(update_data).eq("sku", sku).execute()
        except Exception as e:
            results["errors"].append(f"DB update failed: {str(e)}")

    return {"success": True, "results": results}


@app.post("/sync-all-images")
async def sync_all_images(request: Request):
    """Process all inventory products with external images, sequentially."""
    if WEBHOOK_SECRET and request.headers.get("x-webhook-secret") != WEBHOOK_SECRET:
        return Response(status_code=401, content="Unauthorized")

    # Fetch all products with images
    offset = 0
    batch_size = 100
    all_products = []
    while True:
        resp = supabase.table("inventory").select("sku, product_images, showroom_images").range(offset, offset + batch_size - 1).execute()
        if not resp.data:
            break
        all_products.extend(resp.data)
        if len(resp.data) < batch_size:
            break
        offset += batch_size

    # Filter to products with external images
    to_process = []
    for p in all_products:
        has_external = False
        for img in (p.get("product_images") or []):
            if img and not is_supabase_url(img):
                has_external = True
                break
        if not has_external:
            for img in (p.get("showroom_images") or []):
                if img and not is_supabase_url(img):
                    has_external = True
                    break
        if has_external:
            to_process.append(p)

    summary = {"total": len(to_process), "processed": 0, "failed": 0, "errors": []}

    async with httpx.AsyncClient() as client:
        for product in to_process:
            sku = product["sku"]
            try:
                product_images = product.get("product_images") or []
                showroom_images = product.get("showroom_images") or []
                folder = sku_to_folder(sku)

                items = []
                for i, url in enumerate(product_images):
                    if url and not is_supabase_url(url):
                        items.append({"url": url, "base": f"{folder}/product_{i+1}", "type": "product"})
                for i, url in enumerate(showroom_images):
                    if url and not is_supabase_url(url):
                        items.append({"url": url, "base": f"{folder}/showroom_{i+1}", "type": "showroom"})

                new_product_urls = []
                new_showroom_urls = []

                for item in items:
                    raw_bytes, content_type = await download_image(client, item["url"])
                    converted, ext, mime = convert_image(raw_bytes, item["url"], content_type)
                    full_path = item["base"] + ext
                    public_url = upload_image(full_path, converted, mime)
                    if item["type"] == "product":
                        new_product_urls.append(public_url)
                    else:
                        new_showroom_urls.append(public_url)

                update_data = {}
                if new_product_urls:
                    final = []
                    url_iter = iter(new_product_urls)
                    for url in product_images:
                        if is_supabase_url(url):
                            final.append(url)
                        else:
                            final.append(next(url_iter, url))
                    update_data["product_images"] = final
                if new_showroom_urls:
                    final = []
                    url_iter = iter(new_showroom_urls)
                    for url in showroom_images:
                        if is_supabase_url(url):
                            final.append(url)
                        else:
                            final.append(next(url_iter, url))
                    update_data["showroom_images"] = final

                if update_data:
                    supabase.table("inventory").update(update_data).eq("sku", sku).execute()

                summary["processed"] += 1
            except Exception as e:
                summary["failed"] += 1
                summary["errors"].append(f"{sku}: {str(e)}")

    return {"success": True, "summary": summary}
